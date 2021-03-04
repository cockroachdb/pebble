// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"go/scanner"
	"go/token"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

type methodInfo struct {
	constructor func() op
	validTags   uint32
}

func makeMethod(i interface{}, tags ...objTag) *methodInfo {
	var validTags uint32
	for _, tag := range tags {
		validTags |= 1 << tag
	}

	t := reflect.TypeOf(i)
	return &methodInfo{
		constructor: func() op {
			return reflect.New(t).Interface().(op)
		},
		validTags: validTags,
	}
}

// args returns the receiverID, targetID and arguments for the op. The
// receiverID is the ID of the object the op will be applied to. The targetID
// is the ID of the object for assignment. If the method does not return a new
// object, then targetID will be nil. The argument list is just what it sounds
// like: the list of arguments for the operation.
func opArgs(op op) (receiverID *objID, targetID *objID, args []interface{}) {
	switch t := op.(type) {
	case *applyOp:
		return &t.writerID, nil, []interface{}{&t.batchID}
	case *checkpointOp:
		return nil, nil, nil
	case *closeOp:
		return &t.objID, nil, nil
	case *compactOp:
		return nil, nil, []interface{}{&t.start, &t.end}
	case *batchCommitOp:
		return &t.batchID, nil, nil
	case *dbRestartOp:
		return nil, nil, nil
	case *deleteOp:
		return &t.writerID, nil, []interface{}{&t.key}
	case *deleteRangeOp:
		return &t.writerID, nil, []interface{}{&t.start, &t.end}
	case *iterFirstOp:
		return &t.iterID, nil, nil
	case *flushOp:
		return nil, nil, nil
	case *getOp:
		return &t.readerID, nil, []interface{}{&t.key}
	case *ingestOp:
		return nil, nil, []interface{}{&t.batchIDs}
	case *initOp:
		return nil, nil, []interface{}{&t.batchSlots, &t.iterSlots, &t.snapshotSlots}
	case *iterLastOp:
		return &t.iterID, nil, nil
	case *mergeOp:
		return &t.writerID, nil, []interface{}{&t.key, &t.value}
	case *newBatchOp:
		return nil, &t.batchID, nil
	case *newIndexedBatchOp:
		return nil, &t.batchID, nil
	case *newIterOp:
		return &t.readerID, &t.iterID, []interface{}{&t.lower, &t.upper}
	case *newIterUsingCloneOp:
		return &t.existingIterID, &t.iterID, nil
	case *newSnapshotOp:
		return nil, &t.snapID, nil
	case *iterNextOp:
		return &t.iterID, nil, nil
	case *iterPrevOp:
		return &t.iterID, nil, nil
	case *iterSeekLTOp:
		return &t.iterID, nil, []interface{}{&t.key}
	case *iterSeekGEOp:
		return &t.iterID, nil, []interface{}{&t.key}
	case *iterSeekPrefixGEOp:
		return &t.iterID, nil, []interface{}{&t.key}
	case *setOp:
		return &t.writerID, nil, []interface{}{&t.key, &t.value}
	case *iterSetBoundsOp:
		return &t.iterID, nil, []interface{}{&t.lower, &t.upper}
	case *singleDeleteOp:
		return &t.writerID, nil, []interface{}{&t.key}
	}
	panic(fmt.Sprintf("unsupported op type: %T", op))
}

var methods = map[string]*methodInfo{
	"Apply":           makeMethod(applyOp{}, dbTag, batchTag),
	"Checkpoint":      makeMethod(checkpointOp{}, dbTag),
	"Clone":           makeMethod(newIterUsingCloneOp{}, iterTag),
	"Close":           makeMethod(closeOp{}, dbTag, batchTag, iterTag, snapTag),
	"Commit":          makeMethod(batchCommitOp{}, batchTag),
	"Compact":         makeMethod(compactOp{}, dbTag),
	"Delete":          makeMethod(deleteOp{}, dbTag, batchTag),
	"DeleteRange":     makeMethod(deleteRangeOp{}, dbTag, batchTag),
	"First":           makeMethod(iterFirstOp{}, iterTag),
	"Flush":           makeMethod(flushOp{}, dbTag),
	"Get":             makeMethod(getOp{}, dbTag, batchTag, snapTag),
	"Ingest":          makeMethod(ingestOp{}, dbTag),
	"Init":            makeMethod(initOp{}, dbTag),
	"Last":            makeMethod(iterLastOp{}, iterTag),
	"Merge":           makeMethod(mergeOp{}, dbTag, batchTag),
	"NewBatch":        makeMethod(newBatchOp{}, dbTag),
	"NewIndexedBatch": makeMethod(newIndexedBatchOp{}, dbTag),
	"NewIter":         makeMethod(newIterOp{}, dbTag, batchTag, snapTag),
	"NewSnapshot":     makeMethod(newSnapshotOp{}, dbTag),
	"Next":            makeMethod(iterNextOp{}, iterTag),
	"Prev":            makeMethod(iterPrevOp{}, iterTag),
	"Restart":         makeMethod(dbRestartOp{}, dbTag),
	"SeekGE":          makeMethod(iterSeekGEOp{}, iterTag),
	"SeekLT":          makeMethod(iterSeekLTOp{}, iterTag),
	"SeekPrefixGE":    makeMethod(iterSeekPrefixGEOp{}, iterTag),
	"Set":             makeMethod(setOp{}, dbTag, batchTag),
	"SetBounds":       makeMethod(iterSetBoundsOp{}, iterTag),
	"SingleDelete":    makeMethod(singleDeleteOp{}, dbTag, batchTag),
}

type parser struct {
	fset *token.FileSet
	s    scanner.Scanner
	objs map[objID]bool
}

func parse(src []byte) (_ []op, err error) {
	// Various bits of magic incantation to set up a scanner for Go compatible
	// syntax. We arranged for the textual format of ops (e.g. op.String()) to
	// look like Go which allows us to use the Go scanner for parsing.
	p := &parser{
		fset: token.NewFileSet(),
		objs: map[objID]bool{makeObjID(dbTag, 0): true},
	}
	file := p.fset.AddFile("", -1, len(src))
	p.s.Init(file, src, nil /* no error handler */, 0)
	return p.parse()
}

func (p *parser) parse() (_ []op, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(error); ok {
				return
			}
			err = errors.Errorf("%v", r)
		}
	}()

	var ops []op
	for {
		op := p.parseOp()
		if op == nil {
			return ops, nil
		}
		ops = append(ops, op)
	}
}

func (p *parser) parseOp() op {
	destPos, destTok, destLit := p.s.Scan()
	if destTok == token.EOF {
		return nil
	}
	if destTok != token.IDENT {
		panic(p.errorf(destPos, "unexpected token: %s %q", destTok, destLit))
	}
	if destLit == "Init" {
		// <op>(<args>)
		return p.makeOp(destLit, makeObjID(dbTag, 0), 0, destPos)
	}

	destID := p.parseObjID(destPos, destLit)

	pos, tok, lit := p.s.Scan()
	switch tok {
	case token.PERIOD:
		// <obj>.<op>(<args>)
		if !p.objs[destID] {
			panic(p.errorf(destPos, "unknown object: %s", destID))
		}
		_, methodLit := p.scanToken(token.IDENT)
		return p.makeOp(methodLit, destID, 0, destPos)

	case token.ASSIGN:
		// <obj> = <obj>.<op>(<args>)
		srcPos, srcLit := p.scanToken(token.IDENT)
		srcID := p.parseObjID(srcPos, srcLit)
		if !p.objs[srcID] {
			panic(p.errorf(srcPos, "unknown object %q", srcLit))
		}
		p.scanToken(token.PERIOD)
		_, methodLit := p.scanToken(token.IDENT)
		p.objs[destID] = true
		return p.makeOp(methodLit, srcID, destID, srcPos)
	}
	panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
}

func (p *parser) parseObjID(pos token.Pos, str string) objID {
	var tag objTag
	switch {
	case str == "db":
		return makeObjID(dbTag, 0)
	case strings.HasPrefix(str, "batch"):
		tag, str = batchTag, str[5:]
	case strings.HasPrefix(str, "iter"):
		tag, str = iterTag, str[4:]
	case strings.HasPrefix(str, "snap"):
		tag, str = snapTag, str[4:]
	default:
		panic(p.errorf(pos, "unable to parse objectID: %q", str))
	}
	id, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		panic(p.errorf(pos, "%s", err))
	}
	return makeObjID(tag, uint32(id))
}

func (p *parser) parseArgs(op op, methodName string, args []interface{}) {
	pos, _ := p.scanToken(token.LPAREN)
	for i := range args {
		if i > 0 {
			pos, _ = p.scanToken(token.COMMA)
		}

		switch t := args[i].(type) {
		case *uint32:
			_, lit := p.scanToken(token.INT)
			val, err := strconv.ParseUint(lit, 0, 32)
			if err != nil {
				panic(err)
			}
			*t = uint32(val)

		case *[]byte:
			_, lit := p.scanToken(token.STRING)
			s, err := strconv.Unquote(lit)
			if err != nil {
				panic(err)
			}
			if len(s) == 0 {
				*t = nil
			} else {
				*t = []byte(s)
			}

		case *objID:
			pos, lit := p.scanToken(token.IDENT)
			*t = p.parseObjID(pos, lit)

		case *[]objID:
			for {
				pos, tok, lit := p.s.Scan()
				switch tok {
				case token.IDENT:
					*t = append(*t, p.parseObjID(pos, lit))
					pos, tok, lit := p.s.Scan()
					switch tok {
					case token.COMMA:
						continue
					case token.RPAREN:
						p.scanToken(token.SEMICOLON)
						return
					default:
						panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
					}
				case token.RPAREN:
					p.scanToken(token.SEMICOLON)
					return
				default:
					panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
				}
			}

		default:
			panic(p.errorf(pos, "%s: unsupported arg[%d] type: %T", methodName, i, args[i]))
		}
	}
	p.scanToken(token.RPAREN)
	p.scanToken(token.SEMICOLON)
}

func (p *parser) scanToken(expected token.Token) (pos token.Pos, lit string) {
	pos, tok, lit := p.s.Scan()
	if tok != expected {
		panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
	}
	return pos, lit
}

func (p *parser) makeOp(methodName string, receiverID, targetID objID, pos token.Pos) op {
	info := methods[methodName]
	if info == nil {
		panic(p.errorf(pos, "unknown op %s.%s", receiverID, methodName))
	}
	if info.validTags&(1<<receiverID.tag()) == 0 {
		panic(p.errorf(pos, "%s.%s: %s is not a method on %s",
			receiverID, methodName, methodName, receiverID))
	}

	op := info.constructor()
	receiver, target, args := opArgs(op)

	// The form of an operation is:
	//   [target =] receiver.method(args)
	//
	// The receiver is the object the operation will be called on, which can be
	// any valid ID. Certain operations such as Ingest are only valid on the DB
	// object. That is indicated by opArgs returning a nil receiver.
	if receiver != nil {
		*receiver = receiverID
	} else if receiverID.tag() != dbTag {
		panic(p.errorf(pos, "unknown op %s.%s", receiverID, methodName))
	}

	// The target is the object that will be assigned the result of an object
	// creation operation such as newBatchOp or newIterOp.
	if target != nil {
		// It is invalid to not have a targetID for a method which generates a new
		// object.
		if targetID == 0 {
			panic(p.errorf(pos, "assignment expected for %s.%s", receiverID, methodName))
		}
		// It is invalid to try to assign to the DB object.
		if targetID.tag() == dbTag {
			panic(p.errorf(pos, "cannot use %s as target of assignment", targetID))
		}
		*target = targetID
	} else if targetID != 0 {
		panic(p.errorf(pos, "cannot use %s.%s in assignment", receiverID, methodName))
	}

	p.parseArgs(op, methodName, args)
	return op
}

func (p *parser) tokenf(tok token.Token, lit string) string {
	if tok.IsLiteral() {
		return lit
	}
	return tok.String()
}

func (p *parser) errorf(pos token.Pos, format string, args ...interface{}) error {
	return errors.New(p.fset.Position(pos).String() + ": " + fmt.Sprintf(format, args...))
}

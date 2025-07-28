// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"go/scanner"
	"go/token"
	"reflect"
	"slices"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable/blockiter"
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
// object, then targetID will be nil.
//
// The argument list returns pointers to operation fields that map to arguments
// for the operation. The last argument can be a pointer to a slice,
// corresponding to a variable number of arguments.
func opArgs(op op) (receiverID *objID, targetID *objID, args []interface{}) {
	switch t := op.(type) {
	case *applyOp:
		return &t.writerID, nil, []interface{}{&t.batchID}
	case *checkpointOp:
		return &t.dbID, nil, []interface{}{&t.spans}
	case *closeOp:
		return &t.objID, nil, nil
	case *compactOp:
		return &t.dbID, nil, []interface{}{&t.start, &t.end, &t.parallelize}
	case *batchCommitOp:
		return &t.batchID, nil, nil
	case *dbRatchetFormatMajorVersionOp:
		return &t.dbID, nil, []interface{}{&t.vers}
	case *dbRestartOp:
		return &t.dbID, nil, nil
	case *deleteOp:
		return &t.writerID, nil, []interface{}{&t.key}
	case *deleteRangeOp:
		return &t.writerID, nil, []interface{}{&t.start, &t.end}
	case *downloadOp:
		return &t.dbID, nil, []interface{}{&t.spans}
	case *iterFirstOp:
		return &t.iterID, nil, nil
	case *flushOp:
		return &t.db, nil, nil
	case *getOp:
		return &t.readerID, nil, []interface{}{&t.key}
	case *ingestOp:
		return &t.dbID, nil, []interface{}{&t.batchIDs}
	case *ingestAndExciseOp:
		return &t.dbID, nil, []interface{}{&t.batchID, &t.exciseStart, &t.exciseEnd, ignoreExtraArgs{}}
	case *ingestExternalFilesOp:
		return &t.dbID, nil, []interface{}{&t.objs}
	case *initOp:
		return nil, nil, []interface{}{&t.dbSlots, &t.batchSlots, &t.iterSlots, &t.snapshotSlots, &t.externalObjSlots}
	case *iterLastOp:
		return &t.iterID, nil, nil
	case *logDataOp:
		return &t.writerID, nil, []interface{}{&t.data}
	case *mergeOp:
		return &t.writerID, nil, []interface{}{&t.key, &t.value}
	case *newBatchOp:
		return &t.dbID, &t.batchID, nil
	case *newIndexedBatchOp:
		return &t.dbID, &t.batchID, nil
	case *newIterOp:
		return &t.readerID, &t.iterID, []interface{}{&t.lower, &t.upper, &t.keyTypes, &t.filterMax, &t.filterMin, &t.useL6Filters, &t.maskSuffix}
	case *newIterUsingCloneOp:
		return &t.existingIterID, &t.iterID, []interface{}{&t.refreshBatch, &t.lower, &t.upper, &t.keyTypes, &t.filterMax, &t.filterMin, &t.useL6Filters, &t.maskSuffix}
	case *newSnapshotOp:
		return &t.dbID, &t.snapID, []interface{}{&t.bounds}
	case *newExternalObjOp:
		return &t.batchID, &t.externalObjID, nil
	case *iterNextOp:
		return &t.iterID, nil, []interface{}{&t.limit}
	case *iterNextPrefixOp:
		return &t.iterID, nil, nil
	case *iterCanSingleDelOp:
		return &t.iterID, nil, []interface{}{}
	case *iterPrevOp:
		return &t.iterID, nil, []interface{}{&t.limit}
	case *iterSeekLTOp:
		return &t.iterID, nil, []interface{}{&t.key, &t.limit}
	case *iterSeekGEOp:
		return &t.iterID, nil, []interface{}{&t.key, &t.limit}
	case *iterSeekPrefixGEOp:
		return &t.iterID, nil, []interface{}{&t.key}
	case *setOp:
		return &t.writerID, nil, []interface{}{&t.key, &t.value}
	case *iterSetBoundsOp:
		return &t.iterID, nil, []interface{}{&t.lower, &t.upper}
	case *iterSetOptionsOp:
		return &t.iterID, nil, []interface{}{&t.lower, &t.upper, &t.keyTypes, &t.filterMax, &t.filterMin, &t.useL6Filters, &t.maskSuffix}
	case *singleDeleteOp:
		return &t.writerID, nil, []interface{}{&t.key, &t.maybeReplaceDelete}
	case *rangeKeyDeleteOp:
		return &t.writerID, nil, []interface{}{&t.start, &t.end}
	case *rangeKeySetOp:
		return &t.writerID, nil, []interface{}{&t.start, &t.end, &t.suffix, &t.value}
	case *rangeKeyUnsetOp:
		return &t.writerID, nil, []interface{}{&t.start, &t.end, &t.suffix}
	case *replicateOp:
		return &t.source, nil, []interface{}{&t.dest, &t.start, &t.end}
	}
	panic(fmt.Sprintf("unsupported op type: %T", op))
}

// ignoreExtraArgs is used as a stand-in for a variable length argument for
// cases where we want to ignore additional arguments; used to support
// mixed-version testing when a previous version had an extra argument.
type ignoreExtraArgs struct{}

var methods = map[string]*methodInfo{
	"Apply":                     makeMethod(applyOp{}, dbTag, batchTag),
	"Checkpoint":                makeMethod(checkpointOp{}, dbTag),
	"Clone":                     makeMethod(newIterUsingCloneOp{}, iterTag),
	"Close":                     makeMethod(closeOp{}, dbTag, batchTag, iterTag, snapTag),
	"Commit":                    makeMethod(batchCommitOp{}, batchTag),
	"Compact":                   makeMethod(compactOp{}, dbTag),
	"Delete":                    makeMethod(deleteOp{}, dbTag, batchTag),
	"DeleteRange":               makeMethod(deleteRangeOp{}, dbTag, batchTag),
	"Download":                  makeMethod(downloadOp{}, dbTag),
	"First":                     makeMethod(iterFirstOp{}, iterTag),
	"Flush":                     makeMethod(flushOp{}, dbTag),
	"Get":                       makeMethod(getOp{}, dbTag, batchTag, snapTag),
	"Ingest":                    makeMethod(ingestOp{}, dbTag),
	"IngestAndExcise":           makeMethod(ingestAndExciseOp{}, dbTag),
	"IngestExternalFiles":       makeMethod(ingestExternalFilesOp{}, dbTag),
	"Init":                      makeMethod(initOp{}, dbTag),
	"Last":                      makeMethod(iterLastOp{}, iterTag),
	"LogData":                   makeMethod(logDataOp{}, dbTag, batchTag),
	"Merge":                     makeMethod(mergeOp{}, dbTag, batchTag),
	"NewBatch":                  makeMethod(newBatchOp{}, dbTag),
	"NewIndexedBatch":           makeMethod(newIndexedBatchOp{}, dbTag),
	"NewIter":                   makeMethod(newIterOp{}, dbTag, batchTag, snapTag),
	"NewSnapshot":               makeMethod(newSnapshotOp{}, dbTag),
	"NewExternalObj":            makeMethod(newExternalObjOp{}, batchTag),
	"Next":                      makeMethod(iterNextOp{}, iterTag),
	"NextPrefix":                makeMethod(iterNextPrefixOp{}, iterTag),
	"InternalNext":              makeMethod(iterCanSingleDelOp{}, iterTag),
	"Prev":                      makeMethod(iterPrevOp{}, iterTag),
	"RangeKeyDelete":            makeMethod(rangeKeyDeleteOp{}, dbTag, batchTag),
	"RangeKeySet":               makeMethod(rangeKeySetOp{}, dbTag, batchTag),
	"RangeKeyUnset":             makeMethod(rangeKeyUnsetOp{}, dbTag, batchTag),
	"RatchetFormatMajorVersion": makeMethod(dbRatchetFormatMajorVersionOp{}, dbTag),
	"Replicate":                 makeMethod(replicateOp{}, dbTag),
	"Restart":                   makeMethod(dbRestartOp{}, dbTag),
	"SeekGE":                    makeMethod(iterSeekGEOp{}, iterTag),
	"SeekLT":                    makeMethod(iterSeekLTOp{}, iterTag),
	"SeekPrefixGE":              makeMethod(iterSeekPrefixGEOp{}, iterTag),
	"Set":                       makeMethod(setOp{}, dbTag, batchTag),
	"SetBounds":                 makeMethod(iterSetBoundsOp{}, iterTag),
	"SetOptions":                makeMethod(iterSetOptionsOp{}, iterTag),
	"SingleDelete":              makeMethod(singleDeleteOp{}, dbTag, batchTag),
}

type parser struct {
	opts parserOpts
	fset *token.FileSet
	s    scanner.Scanner
	objs map[objID]bool
}

type parserOpts struct {
	allowUndefinedObjs          bool
	parseFormattedUserKey       func(string) UserKey
	parseFormattedUserKeySuffix func(string) UserKeySuffix
}

func parse(src []byte, opts parserOpts) (_ []op, err error) {
	// Various bits of magic incantation to set up a scanner for Go compatible
	// syntax. We arranged for the textual format of ops (e.g. op.String()) to
	// look like Go which allows us to use the Go scanner for parsing.
	p := &parser{
		opts: opts,
		fset: token.NewFileSet(),
		objs: map[objID]bool{makeObjID(dbTag, 1): true, makeObjID(dbTag, 2): true},
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
			computeDerivedFields(ops)
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
		return p.makeOp(destLit, makeObjID(dbTag, 1), 0, destPos)
	}

	destID := p.parseObjID(destPos, destLit)

	pos, tok, lit := p.s.Scan()
	switch tok {
	case token.PERIOD:
		// <obj>.<op>(<args>)
		if !p.objs[destID] {
			if p.opts.allowUndefinedObjs {
				p.objs[destID] = true
			} else {
				panic(p.errorf(destPos, "unknown object: %s", destID))
			}
		}
		_, methodLit := p.scanToken(token.IDENT)
		return p.makeOp(methodLit, destID, 0, destPos)

	case token.ASSIGN:
		// <obj> = <obj>.<op>(<args>)
		srcPos, srcLit := p.scanToken(token.IDENT)
		srcID := p.parseObjID(srcPos, srcLit)
		if !p.objs[srcID] {
			if p.opts.allowUndefinedObjs {
				p.objs[srcID] = true
			} else {
				panic(p.errorf(srcPos, "unknown object %q", srcLit))
			}
		}
		p.scanToken(token.PERIOD)
		_, methodLit := p.scanToken(token.IDENT)
		p.objs[destID] = true
		return p.makeOp(methodLit, srcID, destID, srcPos)
	}
	panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
}

func (p *parser) parseObjID(pos token.Pos, str string) objID {
	id, err := parseObjID(str)
	if err != nil {
		panic(p.errorf(pos, "%s", err))
	}
	return id
}

func (p *parser) parseUserKey(pos token.Pos, lit string) UserKey {
	s, err := strconv.Unquote(lit)
	if err != nil {
		panic(p.errorf(pos, "%s", err))
	}
	if len(s) == 0 {
		return nil
	}
	if p.opts.parseFormattedUserKey == nil {
		return []byte(s)
	}
	return p.opts.parseFormattedUserKey(s)
}

func (p *parser) parseUserKeySuffix(pos token.Pos, lit string) UserKeySuffix {
	s, err := strconv.Unquote(lit)
	if err != nil {
		panic(p.errorf(pos, "%s", err))
	}
	if len(s) == 0 {
		return nil
	}
	if p.opts.parseFormattedUserKeySuffix == nil {
		return []byte(s)
	}
	return p.opts.parseFormattedUserKeySuffix(s)
}

func unquoteBytes(lit string) []byte {
	s, err := strconv.Unquote(lit)
	if err != nil {
		panic(err)
	}
	if len(s) == 0 {
		return nil
	}
	return []byte(s)
}

func (p *parser) parseArgs(op op, methodName string, args []interface{}) {
	pos, list := p.parseList()
	p.scanToken(token.SEMICOLON)

	// The last argument can have variable length.
	var varArg interface{}
	if len(args) > 0 {
		switch args[len(args)-1].(type) {
		case *[]objID, *[]pebble.KeyRange, *[]pebble.CheckpointSpan, *[]pebble.DownloadSpan, *[]externalObjWithBounds, ignoreExtraArgs:
			varArg = args[len(args)-1]
			args = args[:len(args)-1]
		}
	}

	if len(list) < len(args) {
		panic(p.errorf(pos, "%s: not enough arguments", methodName))
	}
	if len(list) > len(args) && varArg == nil {
		panic(p.errorf(pos, "%s: too many arguments", methodName))
	}

	for i := range args {
		elem := list[i]
		switch t := args[i].(type) {
		case *uint32:
			elem.expectToken(p, token.INT)
			val, err := strconv.ParseUint(elem.lit, 10, 32)
			if err != nil {
				panic(p.errorf(elem.pos, "error parsing %q: %s", elem.lit, err))
			}
			*t = uint32(val)

		case *uint64:
			elem.expectToken(p, token.INT)
			val, err := strconv.ParseUint(elem.lit, 10, 64)
			if err != nil {
				panic(p.errorf(elem.pos, "error parsing %q: %s", elem.lit, err))
			}
			*t = val

		case *UserKey:
			elem.expectToken(p, token.STRING)
			*t = p.parseUserKey(elem.pos, elem.lit)

		case *UserKeySuffix:
			if elem.tok == token.INT {
				// Tolerate integers for backward compatibility (when loading ops from a
				// previous version).
				*t = UserKeySuffix(elem.lit)
			} else {
				elem.expectToken(p, token.STRING)
				*t = p.parseUserKeySuffix(elem.pos, elem.lit)
			}

		case *[]byte:
			elem.expectToken(p, token.STRING)
			*t = unquoteBytes(elem.lit)

		case *bool:
			elem.expectToken(p, token.IDENT)
			b, err := strconv.ParseBool(elem.lit)
			if err != nil {
				panic(p.errorf(elem.pos, "error parsing %q: %s", elem.lit, err))
			}
			*t = b

		case *objID:
			elem.expectToken(p, token.IDENT)
			*t = p.parseObjID(elem.pos, elem.lit)

		case *pebble.FormatMajorVersion:
			elem.expectToken(p, token.INT)
			val, err := strconv.ParseUint(elem.lit, 10, 64)
			if err != nil {
				panic(p.errorf(elem.pos, "error parsing %q: %s", elem.lit, err))
			}
			*t = pebble.FormatMajorVersion(val)

		default:
			panic(p.errorf(pos, "%s: unsupported arg[%d] type: %T", methodName, i, args[i]))
		}
	}

	if varArg != nil {
		list = list[len(args):]
		switch t := varArg.(type) {
		case *[]objID:
			*t = p.parseObjIDs(list)
		case *[]pebble.KeyRange:
			*t = p.parseKeyRanges(list)
		case *[]pebble.CheckpointSpan:
			*t = p.parseCheckpointSpans(list)
		case *[]pebble.DownloadSpan:
			*t = p.parseDownloadSpans(list)
		case *[]externalObjWithBounds:
			*t = p.parseExternalObjsWithBounds(list)
		case ignoreExtraArgs:
		default:
			// We already checked for these types when we set varArgs.
			panic("unreachable")
		}
	}
}

type listElem struct {
	pos token.Pos
	tok token.Token
	lit string
}

func (e listElem) expectToken(p *parser, expTok token.Token) {
	if e.tok != expTok {
		panic(p.errorf(e.pos, "unexpected token: %q", p.tokenf(e.tok, e.lit)))
	}
}

// pop checks that the first element of the list matches the expected token,
// removes it from the list and returns the pos and literal.
func (p *parser) pop(list *[]listElem, expTok token.Token) (token.Pos, string) {
	(*list)[0].expectToken(p, expTok)
	pos := (*list)[0].pos
	lit := (*list)[0].lit
	(*list) = (*list)[1:]
	return pos, lit
}

// popLit checks that the first element of the list matches the expected token,
// removes it from the list and returns the literal.
func (p *parser) popLit(list *[]listElem, expTok token.Token) string {
	(*list)[0].expectToken(p, expTok)
	lit := (*list)[0].lit
	(*list) = (*list)[1:]
	return lit
}

// parseKeyRange parses an arbitrary number of comma separated STRING/IDENT/INT
// tokens surrounded by parens.
func (p *parser) parseList() (token.Pos, []listElem) {
	p.scanToken(token.LPAREN)
	var list []listElem
	for {
		pos, tok, lit := p.s.Scan()
		if len(list) == 0 && tok == token.RPAREN {
			return pos, nil
		}

		switch tok {
		case token.STRING, token.IDENT, token.INT:
			list = append(list, listElem{
				pos: pos,
				tok: tok,
				lit: lit,
			})
			pos, tok, lit = p.s.Scan()
			switch tok {
			case token.COMMA:
				continue
			case token.RPAREN:
				return pos, list
			}
		}
		panic(p.errorf(pos, "unexpected token: %q", p.tokenf(tok, lit)))
	}
}

func (p *parser) parseObjIDs(list []listElem) []objID {
	res := make([]objID, len(list))
	for i, elem := range list {
		res[i] = p.parseObjID(elem.pos, elem.lit)
	}
	return res
}

func (p *parser) parseKeys(list []listElem) []UserKey {
	res := make([]UserKey, len(list))
	for i := range res {
		res[i] = p.parseUserKey(p.pop(&list, token.STRING))
	}
	return res
}

func (p *parser) parseKeyRanges(list []listElem) []pebble.KeyRange {
	keys := p.parseKeys(list)
	if len(keys)%2 != 0 {
		panic(p.errorf(list[0].pos, "expected even number of keys"))
	}
	res := make([]pebble.KeyRange, len(keys)/2)
	for i := range res {
		res[i].Start = keys[2*i]
		res[i].End = keys[2*i+1]
	}
	return res
}

func (p *parser) parseCheckpointSpans(list []listElem) []pebble.CheckpointSpan {
	keys := p.parseKeys(list)
	if len(keys)%2 == 1 {
		panic(p.errorf(list[0].pos, "expected even number of keys"))
	}
	if len(keys) == 0 {
		// Necessary for round-trip tests which differentiate between nil and empty slice.
		return nil
	}
	res := make([]pebble.CheckpointSpan, len(keys)/2)
	for i := range res {
		res[i] = pebble.CheckpointSpan{
			Start: keys[i*2],
			End:   keys[i*2+1],
		}
	}
	return res
}

func (p *parser) parseDownloadSpans(list []listElem) []pebble.DownloadSpan {
	if len(list)%3 != 0 {
		panic(p.errorf(list[0].pos, "expected 3k args"))
	}
	res := make([]pebble.DownloadSpan, len(list)/3)
	for i := range res {
		res[i] = pebble.DownloadSpan{
			StartKey:               p.parseUserKey(p.pop(&list, token.STRING)),
			EndKey:                 p.parseUserKey(p.pop(&list, token.STRING)),
			ViaBackingFileDownload: p.popLit(&list, token.IDENT) == "true",
		}
	}
	return res
}

func (p *parser) parseExternalObjsWithBounds(list []listElem) []externalObjWithBounds {
	const numArgs = 5
	if len(list)%5 != 0 {
		panic(p.errorf(list[0].pos, "expected number of arguments to be multiple of %d", numArgs))
	}
	objs := make([]externalObjWithBounds, len(list)/numArgs)
	for i := range objs {
		objs[i] = externalObjWithBounds{
			externalObjID: p.parseObjID(p.pop(&list, token.IDENT)),
			bounds: pebble.KeyRange{
				Start: p.parseUserKey(p.pop(&list, token.STRING)),
				End:   p.parseUserKey(p.pop(&list, token.STRING)),
			},
		}
		if syntheticSuffix := p.parseUserKeySuffix(p.pop(&list, token.STRING)); len(syntheticSuffix) > 0 {
			objs[i].syntheticSuffix = blockiter.SyntheticSuffix(syntheticSuffix)
		}

		// NB: We cannot use p.parseUserKey for the syntheticPrefix because it
		// is not guaranteed to be a valid key itself. For example, when using
		// the cockroachkvs KeyFormat, it may not have a 0x00 delimiter/sentinel
		// byte.
		syntheticPrefix, err := strconv.Unquote(p.popLit(&list, token.STRING))
		if err != nil {
			panic(p.errorf(list[0].pos, "error parsing synthetic prefix: %s", err))
		}
		if len(syntheticPrefix) > 0 {
			if !bytes.HasPrefix(objs[i].bounds.Start, []byte(syntheticPrefix)) {
				panic(fmt.Sprintf("invalid synthetic prefix %q [%x] %s [%x]",
					objs[i].bounds.Start, []byte(objs[i].bounds.Start), syntheticPrefix, []byte(syntheticPrefix)))
			}
			if !bytes.HasPrefix(objs[i].bounds.End, []byte(syntheticPrefix)) {
				panic(fmt.Sprintf("invalid synthetic prefix %q %s", objs[i].bounds.End, syntheticPrefix))
			}
			objs[i].syntheticPrefix = blockiter.SyntheticPrefix(syntheticPrefix)
		}
	}
	return objs
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

	defer func() {
		if r := recover(); r != nil {
			panic(errors.Newf("parsing %s.%s: %v", receiverID, methodName, r))
		}
	}()

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
	return errors.New("metamorphic test internal error: " + p.fset.Position(pos).String() + ": " + fmt.Sprintf(format, args...))
}

// computeDerivedFields makes one pass through the provided operations, filling
// any derived fields. This pass must happen before execution because concurrent
// execution depends on these fields.
func computeDerivedFields(ops []op) {
	iterToReader := make(map[objID]objID)
	objToDB := make(map[objID]objID)
	for i := range ops {
		switch v := ops[i].(type) {
		case *newSnapshotOp:
			objToDB[v.snapID] = v.dbID
		case *newIterOp:
			iterToReader[v.iterID] = v.readerID
			dbReaderID := v.readerID
			if dbReaderID.tag() != dbTag {
				dbReaderID = objToDB[dbReaderID]
			}
			objToDB[v.iterID] = dbReaderID
			v.derivedDBID = dbReaderID
		case *newIterUsingCloneOp:
			v.derivedReaderID = iterToReader[v.existingIterID]
			iterToReader[v.iterID] = v.derivedReaderID
			objToDB[v.iterID] = objToDB[v.existingIterID]
		case *iterSetOptionsOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterFirstOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterLastOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterSeekGEOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterSeekPrefixGEOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterSeekLTOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterNextOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterNextPrefixOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterCanSingleDelOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *iterPrevOp:
			v.derivedReaderID = iterToReader[v.iterID]
		case *newBatchOp:
			objToDB[v.batchID] = v.dbID
		case *newIndexedBatchOp:
			objToDB[v.batchID] = v.dbID
		case *applyOp:
			if derivedDBID, ok := objToDB[v.batchID]; ok && v.writerID.tag() != dbTag {
				objToDB[v.writerID] = derivedDBID
			}
		case *getOp:
			if derivedDBID, ok := objToDB[v.readerID]; ok {
				v.derivedDBID = derivedDBID
			}
		case *batchCommitOp:
			v.dbID = objToDB[v.batchID]
		case *closeOp:
			if v.objID.tag() == dbTag {
				// Find all objects that use this db.
				v.affectedObjects = nil
				for obj, db := range objToDB {
					if db == v.objID {
						v.affectedObjects = append(v.affectedObjects, obj)
					}
				}
				// Sort so the output is deterministic.
				slices.Sort(v.affectedObjects)
			} else if dbID, ok := objToDB[v.objID]; ok {
				v.affectedObjects = objIDSlice{dbID}
			}
		case *dbRestartOp:
			// Find all objects that use this db.
			v.affectedObjects = nil
			for obj, db := range objToDB {
				if db == v.dbID {
					v.affectedObjects = append(v.affectedObjects, obj)
				}
			}
			// Sort so the output is deterministic.
			slices.Sort(v.affectedObjects)
		case *ingestOp:
			v.derivedDBIDs = make([]objID, len(v.batchIDs))
			for i := range v.batchIDs {
				v.derivedDBIDs[i] = objToDB[v.batchIDs[i]]
			}
		case *ingestAndExciseOp:
			v.derivedDBID = objToDB[v.batchID]
		case *deleteOp:
			derivedDBID := v.writerID
			if v.writerID.tag() != dbTag {
				derivedDBID = objToDB[v.writerID]
			}
			v.derivedDBID = derivedDBID
		}
	}
}

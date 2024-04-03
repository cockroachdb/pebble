// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lsmview

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"net/url"
)

// GenerateURL generates a URL showing the LSM diagram. The URL contains the
// encoded and compressed data as the URL fragment.
func GenerateURL(data Data) (url.URL, error) {
	var jsonBuf bytes.Buffer
	if err := json.NewEncoder(&jsonBuf).Encode(data); err != nil {
		return url.URL{}, err
	}

	var compressed bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := jsonBuf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	return url.URL{
		Scheme:   "https",
		Host:     "raduberinde.github.io",
		Path:     "lsmview/decode.html",
		Fragment: compressed.String(),
	}, nil
}

package drain

import (
	"bytes"
	"strings"

	"github.com/go-logfmt/logfmt"

	"github.com/grafana/loki/v3/pkg/pattern/tokenization"
)

type PatternTokenizer interface {
	Marshal([]byte) [][]byte
	Unmarshal([]string) string
}

// AdaptiveTokenizer is a tokenizer using the same methodology as the Adaptive Logs feature.
// It replaces as much as possible with constants before passing to Drain (e.g. <NUM>, <DURATION>, <TIMESTAMP>, <HEX>)
// It tokenizes multi-word quotes as single tokens.
type AdaptiveTokenizer struct{}

func (a *AdaptiveTokenizer) Marshal(in []byte) [][]byte {
	return tokenization.PreprocessAndTokenizeBytesWithOpts(in, tokenization.TokenizerOpts{
		UseSingleTokenForQuotes:   true,
		IncludeDelimitersInTokens: true,
		PreprocessNumbers:         false,
		PreprocessHex:             false,
	})
}

func (a *AdaptiveTokenizer) Unmarshal(tokens []string) string {
	return strings.Join(tokens, "")
}

// LogfmtTokenizer is designed for Logfmt formatted logs.
// It splits lines into separate tokens for keys and values.
// tokenizerInsideQuotes splits values with multiple words into separate tokens by spaces.
type LogfmtTokenizer struct {
	tokenizeInsideQuotes bool
	bytesReader          *bytes.Reader
	bufs                 [][]byte
}

func NewLogFmtTokenizer(tokenizeInsideQuotes bool) *LogfmtTokenizer {
	return &LogfmtTokenizer{
		tokenizeInsideQuotes: tokenizeInsideQuotes,
		bytesReader:          bytes.NewReader(nil),
		bufs:                 make([][]byte, 0, 128), // Can process a maximum of 128 tokens before
	}
}

// Marshal accepts a log line in []byte form and splits it into tokens
// The result from Marshal is only valid until the next call to Marshal as buffers will be re-used to avoid unnecessary allocations.
func (a *LogfmtTokenizer) Marshal(in []byte) [][]byte {
	tokens := a.bufs
	processed := tokenization.Preprocess(in, false, false) // Returns a new byte buffer after tokenisation
	a.bytesReader.Reset(processed)
	decoder := logfmt.NewDecoder(a.bytesReader)

	for decoder.ScanRecord() {
		for decoder.ScanKeyval() {
			k := decoder.Key()
			v := decoder.Value()
			if v == nil {
				tokens = append(tokens, k)
				continue
			}
			tokens = append(tokens, append(k, byte('=')))

			numSpaces := bytes.Count(v, []byte(" "))
			if a.tokenizeInsideQuotes && numSpaces > 0 {
				tokens = append(tokens, bytes.Split(v, []byte(" "))...)
			} else {
				tokens = append(tokens, v)
			}
		}
	}

	return tokens
}

func (a *LogfmtTokenizer) Unmarshal(tokens []string) string {
	var output strings.Builder
	var value strings.Builder
	var valueTokens int

	for _, token := range tokens {
		if strings.HasSuffix(token, "=") {
			fullValue := value.String()
			if valueTokens > 1 {
				output.WriteString("\"")
				output.WriteString(fullValue[:len(fullValue)-1]) // Drop the trailing space
				output.WriteString("\" ")
			} else {
				output.WriteString(fullValue)
			}
			value.Reset()
			valueTokens = 0
			output.WriteString(token)
		} else {
			value.WriteString(token)
			value.WriteString(" ")
			valueTokens++
		}
	}
	fullValue := value.String()
	fullValue = fullValue[:len(fullValue)-1] // Drop trailing space from final value
	if valueTokens > 1 {
		output.WriteString("\"")
		output.WriteString(fullValue)
		output.WriteString("\"")
	} else {
		output.WriteString(fullValue)
	}
	return output.String()
}

// LogfmtTokenizer is designed for Logfmt formatted logs.
// It splits lines into separate tokens for keys and values.
// tokenizerInsideQuotes splits values with multiple words into separate tokens by spaces.
type ExpLogfmtTokenizer struct {
	tokenBuffer          [][]byte
	tempEqualSplitBuffer [][]byte
	tempSpaceSplitBuffer [][]byte
}

// Copy of bytes.genSplit but with a pre-allocated buffer to save on allocations
// Some guard cases have been removed for our usage
func (e *ExpLogfmtTokenizer) genSplit(s, sep []byte, sepSave int, a [][]byte) [][]byte {
	n := bytes.Count(s, sep) + 1

	n--
	i := 0
	for i < n {
		m := bytes.Index(s, sep)
		if m < 0 {
			break
		}
		a[i] = s[: m+sepSave : m+sepSave]
		s = s[m+len(sep):]
		i++
	}
	a[i] = s
	return a[:i+1]
}

func NewExpLogfmtTokenizer() *ExpLogfmtTokenizer {
	return &ExpLogfmtTokenizer{
		tokenBuffer:          make([][]byte, 0, 1024),
		tempEqualSplitBuffer: make([][]byte, 1024),
		tempSpaceSplitBuffer: make([][]byte, 1024),
	}
}

// Marshal accepts a log line in []byte form and splits it into tokens
// The result from Marshal is only valid until the next call to Marshal as buffers will be re-used to avoid unnecessary allocations.
func (a *ExpLogfmtTokenizer) Marshal(in []byte) [][]byte {
	tokens := a.tokenBuffer
	processed := tokenization.Preprocess(in, false, false) // Returns a new byte buffer after tokenisation

	for _, slice := range a.genSplit(processed, []byte("="), 1, a.tempEqualSplitBuffer) {
		tokens = append(tokens, a.genSplit(slice, []byte(" "), 0, a.tempSpaceSplitBuffer)...)
	}

	return tokens
}
func (a *ExpLogfmtTokenizer) Unmarshal(tokens []string) string {
	var output strings.Builder

	for _, token := range tokens {
		if strings.HasSuffix(token, "=") {
			output.WriteString(token)
		} else {
			output.WriteString(token)
			output.WriteString(" ")
		}
	}
	result := output.String()
	if result[len(result)-1] == ' ' {
		return result[:len(result)-1]
	}
	return result
}

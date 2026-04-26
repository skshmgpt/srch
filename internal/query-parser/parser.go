package queryparser

import (
	"strings"
	"unicode"
)

func tokenize(query string) []string {
	tokens := []string{}
	sb := strings.Builder{}
	for _, r := range query {
		r := unicode.ToLower(r)
		if unicode.IsLetter(r) {
			sb.WriteRune(r)
		} else {
			tok := sb.String()
			if tok != "" {
				tokens = append(tokens, tok)
			}
		}
	}

	tok := sb.String()
	if tok != "" {
		tokens = append(tokens, tok)
	}
	return tokens
}

// func Parse(query string) {
// 	tokens := tokenize(query)

// }

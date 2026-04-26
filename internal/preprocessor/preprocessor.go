// Package preprocessor strips MediaWiki markup down to roughly plain prose
// so the tokenizer doesn't index template names, ref tags, file captions,
// table syntax, etc. Not a faithful wikitext parser — targets ~95% noise
// reduction for IR purposes.
package preprocessor

import (
	"html"
	"regexp"
	"strings"
)

var (
	// Order matters: comments and refs first (they can contain anything,
	// including markup that would confuse later passes).
	reHTMLComment = regexp.MustCompile(`(?s)<!--.*?-->`)
	reRefPair     = regexp.MustCompile(`(?is)<ref\b[^>]*>.*?</ref>`)
	reRefSelf     = regexp.MustCompile(`(?i)<ref\b[^/>]*/\s*>`)

	// Innermost {{...}} — applied iteratively to peel nested templates.
	reTemplate = regexp.MustCompile(`\{\{[^{}]*\}\}`)
	// Innermost {| ... |} table block.
	reTable = regexp.MustCompile(`(?s)\{\|[^{}]*?\|\}`)

	// File/Image/Category links: regex matches the OPENING `[[Prefix:` only;
	// the matching `]]` is found by stripBracketBlocks via bracket counting,
	// because file captions routinely contain nested [[wikilinks]] that RE2
	// can't balance.
	reFileCatOpen = regexp.MustCompile(`(?i)\[\[(?:File|Image|Category):`)

	// Plain wikilinks: [[Article|display]] -> display, [[Article]] -> Article.
	// Run after FileCat so we don't accidentally unwrap a file link.
	reWikiLink = regexp.MustCompile(`\[\[([^\[\]|]+)(?:\|([^\[\]]*))?\]\]`)

	// External links: [http://... text] -> text, [http://...] -> empty.
	reExtLinkText = regexp.MustCompile(`\[(?:https?|ftp|mailto)[^\s\]]+\s+([^\]]*)\]`)
	reExtLinkBare = regexp.MustCompile(`\[(?:https?|ftp|mailto)[^\]]+\]`)

	// Section headers: ==Foo== / === Foo === -> Foo
	reSection = regexp.MustCompile(`(?m)^=+\s*(.+?)\s*=+\s*$`)

	// Bold/italic markers: '''bold''' / ''italic'' / '''''both'''''
	reTicks = regexp.MustCompile(`'{2,5}`)

	// Generic HTML tags last — by now the only ones left are presentational
	// (sub, sup, br, span, etc.). Strip tags; keep inner text.
	reHTMLTag = regexp.MustCompile(`(?s)<[^>]+>`)

	// Tags whose contents aren't natural language: drop the whole block.
	// One regex per tag because RE2 has no backreferences.
	reCodeBlocks = func() []*regexp.Regexp {
		tags := []string{"math", "gallery", "syntaxhighlight", "source", "code", "pre", "nowiki"}
		out := make([]*regexp.Regexp, 0, len(tags))
		for _, t := range tags {
			out = append(out, regexp.MustCompile(`(?is)<`+t+`\b[^>]*>.*?</\s*`+t+`\s*>`))
			// also self-closing form: <math/>, <gallery .../>
			out = append(out, regexp.MustCompile(`(?i)<`+t+`\b[^>]*/\s*>`))
		}
		return out
	}()

	// Whitespace cleanup
	reHRule    = regexp.MustCompile(`(?m)^----+\s*$`)
	reMultiWS  = regexp.MustCompile(`[ \t]+`)
	reMultiNL  = regexp.MustCompile(`\n{3,}`)
	reLeadList = regexp.MustCompile(`(?m)^[\*#:;]+\s*`)
)

// Strip converts wikitext to roughly plain text. Safe to call on titles too,
// though titles rarely contain markup.
func Strip(s string) string {
	s = reHTMLComment.ReplaceAllString(s, "")
	s = reRefPair.ReplaceAllString(s, "")
	s = reRefSelf.ReplaceAllString(s, "")
	for _, re := range reCodeBlocks {
		s = re.ReplaceAllString(s, "")
	}

	// Templates and tables nest; apply repeatedly until the string stabilizes.
	s = iterReplace(s, reTemplate, "")
	s = iterReplace(s, reTable, "")
	s = stripBracketBlocks(s, reFileCatOpen)

	// Plain wikilinks: keep displayed text or article name.
	s = reWikiLink.ReplaceAllStringFunc(s, func(m string) string {
		sub := reWikiLink.FindStringSubmatch(m)
		if sub[2] != "" {
			return sub[2]
		}
		return sub[1]
	})

	s = reExtLinkText.ReplaceAllString(s, "$1")
	s = reExtLinkBare.ReplaceAllString(s, "")
	s = reSection.ReplaceAllString(s, "$1")
	s = reTicks.ReplaceAllString(s, "")
	s = reHTMLTag.ReplaceAllString(s, "")
	s = reHRule.ReplaceAllString(s, "")
	s = reLeadList.ReplaceAllString(s, "")

	s = html.UnescapeString(s)

	s = reMultiWS.ReplaceAllString(s, " ")
	s = reMultiNL.ReplaceAllString(s, "\n\n")
	return s
}

// iterReplace applies re repeatedly until the string stops changing. Used
// for nested constructs (templates, tables) where one pass only removes the
// innermost match. Bounded to avoid pathological loops.
func iterReplace(s string, re *regexp.Regexp, repl string) string {
	const maxIter = 16
	for range maxIter {
		out := re.ReplaceAllString(s, repl)
		if out == s {
			return out
		}
		s = out
	}
	return s
}

// stripBracketBlocks finds each `openRe` match and deletes from there to the
// matching `]]`, accounting for arbitrarily nested `[[...]]` inside. Required
// for file/image/category links because their captions contain wikilinks and
// RE2 can't balance brackets.
func stripBracketBlocks(s string, openRe *regexp.Regexp) string {
	var b strings.Builder
	b.Grow(len(s))
	cursor := 0
	for {
		loc := openRe.FindStringIndex(s[cursor:])
		if loc == nil {
			b.WriteString(s[cursor:])
			return b.String()
		}
		openStart := cursor + loc[0]
		b.WriteString(s[cursor:openStart])

		// Walk forward from the opening `[[`, tracking bracket depth.
		depth := 1
		i := cursor + loc[1]
		for i < len(s)-1 {
			switch {
			case s[i] == '[' && s[i+1] == '[':
				depth++
				i += 2
			case s[i] == ']' && s[i+1] == ']':
				depth--
				i += 2
				if depth == 0 {
					goto done
				}
			default:
				i++
			}
		}
	done:
		cursor = i
		if cursor > len(s) {
			cursor = len(s)
		}
		if depth > 0 {
			// Unmatched open — drop the rest of the string and stop.
			return b.String()
		}
	}
}

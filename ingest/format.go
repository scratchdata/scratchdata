package ingest

import (
	"bufio"
	"io"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
)

func writeHTML(r io.Reader, c *fiber.Ctx) error {
	md, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// create markdown parser with extensions
	extensions := parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock
	p := parser.NewWithExtensions(extensions)
	doc := p.Parse(md)

	// create HTML renderer with extensions
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	opts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(opts)

	parsed := markdown.Render(doc, renderer)
	c.Set(fiber.HeaderContentType, fiber.MIMETextHTML)
	s := `
		<style>
		table, tr, td, th {border: 1px solid; border-collapse:collapse}
		td,th{padding:3px;}
		</style>
		`

	if _, err = c.WriteString(s); err != nil {
		return err
	}
	if _, err = c.Write(parsed); err != nil {
		return err
	}
	return nil
}

func writeJSON(r io.Reader, c *fiber.Ctx) error {
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	if _, err := c.WriteString("["); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	line, isPrefix, err := reader.ReadLine()
	for len(line) > 0 || err != io.EOF {
		// write current line
		if _, err = c.Write(line); err != nil {
			log.Println(err)
		}

		// read for the next line
		line, isPrefix, err = reader.ReadLine()

		// avoid adding trailing comma at EOF or in a prefix
		if !isPrefix && err != io.EOF {
			if _, err := c.WriteString(","); err != nil {
				log.Println(err)
			}
		}
	}

	if _, err := c.WriteString("]"); err != nil {
		log.Println(err)
	}

	return err
}

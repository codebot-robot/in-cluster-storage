HUGO ?= hugo

.PHONY: preview build clean

preview:
	$(HUGO) server -D --disableFastRender

build:
	$(HUGO) --minify

clean:
	rm -rf public resources/_gen .hugo_build.lock

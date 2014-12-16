lint:
	@./node_modules/.bin/jshint . --verbose

test: lint
	@./node_modules/.bin/mocha test.js

.PHONY : test

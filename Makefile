all:
	bun build index.ts --target=node --outdir=./dist

run:
	node ./dist/index.js
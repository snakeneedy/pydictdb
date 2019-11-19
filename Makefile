MODULE=pydictdb
TEST=tests
SPHINX_SRC=docsrc
GITHUB_HTML=docs

test:
	pytest tests

init:
	pip install -r requirements.txt

doc:
	sphinx-apidoc -o ${SPHINX_SRC} ${MODULE}
	cd ${SPHINX_SRC} && make html

github: doc
	rm -rf ${GITHUB_HTML}
	cp -r ${SPHINX_SRC}/_build/html ${GITHUB_HTML}
	touch ${GITHUB_HTML}/.nojekyll

clean:
	find ${MODULE} -name "__pycache__" -type d -exec rm -r {} +
	find ${TEST} -name "__pycache__" -type d -exec rm -r {} +

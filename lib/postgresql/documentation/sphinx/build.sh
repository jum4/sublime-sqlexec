#!/bin/sh
cd "$(dirname $0)"

# distutils doesn't make it straighforward to include an arbitrary
# directory in the package data, so manage .static and .templates here.
mkdir -p .static .templates
cat >.static/unsuck.css_t <<EOF
th.field-name {
	font-weight: normal;
	background: inherit;
	font-variant: small-caps;
	font-size: small;
}

td.field-body ul li strong {
	font-weight: normal;
	border-bottom-width: 1px;
	border-bottom-style: dashed;
	border-bottom-color: darkgreen;
}
EOF

cat >.templates/layout.html <<EOF
{% extends "!layout.html" %}

{%- block extrahead %}
{{ super() }}
<link rel="stylesheet" href="_static/unsuck.css" type="text/css" />
{% endblock %}
EOF

mkdir -p ../html/doctrees
sphinx-build -c "$(pwd)" -E -b html -d ../html/doctrees .. ../html
cd ../html && pwd

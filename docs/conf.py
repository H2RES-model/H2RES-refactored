import os
import sys

sys.path.insert(0, os.path.abspath(".."))

project = "H2RES Refactored"
author = "H2RES team"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.viewcode",
    "myst_parser",
]

autosummary_generate = True

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}

autodoc_mock_imports = ["pandas", "pyarrow"]

napoleon_google_docstring = True
napoleon_numpy_docstring = False
autosectionlabel_prefix_document = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

source_suffix = {
    ".md": "markdown",
}

html_theme = "furo"
html_static_path = ["_static"]

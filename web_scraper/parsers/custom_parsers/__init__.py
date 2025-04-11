"""
Custom parsers for specific websites.
"""

from . import uscis_parser
from . import unhcr_parser
from . import travel_stackexchange_parser
from . import tracreports_parser
from . import iom_dtm_parser
from . import oecd_parser

# Custom parsers package
# 
# This package contains custom parsers for specific websites related to immigration data.
# Each parser should be implemented as a separate module with either:
# 1. A 'Parser' class with a parse(html_content, url) method, or
# 2. A 'parse(html_content, url)' function
#
# Both approaches should return a dictionary of extracted data. 
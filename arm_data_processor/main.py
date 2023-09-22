import pdfplumber
import logging

logger = logging.getLogger(__name__)

PDF_FILE = "../data/UG_Web_G_HBS_780_US_REV_01_170103.pdf"
# Open the PDF file in binary read mode
with pdfplumber.open(PDF_FILE) as pdf:
    print("PDF file opened")
    print("Number of pages: %s", len(pdf.pages))
    pages = pdf.pages
    first_page = pages[0]
    print(first_page.extract_text())
    # for page in pages:
    # print(page.to_json())
    # text = page.extract_text()
    # print(text)

# Import libraries
import platform, json, logging, os
from pdfminer.pdfpage import PDFPage
from pathlib import Path 
import pandas as pd
from glob import glob
from tqdm import tqdm

# setting up base parameters
PARSER_PARAMS = "./pdf_parser_params.json"
LOGS_FOLDER = "./logs"
LOGNAME = "/log_pdf_parser.log"
REPORT_FOLDER = "reports"
REPORT_NAME = REPORT_FOLDER + "/report_parsing_"

pdf_scanner_paths = json.load(open(PARSER_PARAMS)) # opening json file
os.environ["OMP_THREAD_LIMIT"] = "1" # avoids threading overhead within pytesseract

if platform.system() == "Windows": # reading paths for current os
    PDF_DIRECTORY = pdf_scanner_paths["paths"]["windows_paths"]["pdfs_data"]
else:
    PDF_DIRECTORY = pdf_scanner_paths["paths"]["unix_paths"]["pdfs_data"]

# obtaining parameters
JOB_TYPE = pdf_scanner_paths["parameters"]["job_type"]
PARAMETERS = pdf_scanner_paths["parameters"][JOB_TYPE]
USER_INPUT = PARAMETERS["user_input"]
NUM_THREADS = PARAMETERS["num_threads"]
INITIAL_INDEX = PARAMETERS["initial_index"]
FINAL_INDEX = PARAMETERS["final_index"]

logging.basicConfig(filename=LOGS_FOLDER + LOGNAME,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def folder_creator(folder_name: str, path: str) -> None:
    """
    Generates a folder in specified path
    """
    
    # defining paths
    data_folder_path = path + "/" + folder_name
    data_folder_exists = os.path.exists(data_folder_path)

    # creating folders if don't exist
    if data_folder_exists:
        pass
    else:    
        # create a new directory because it does not exist 
        os.makedirs(data_folder_path)

        # create subfolders
        print(f"The new directory '{folder_name}' was created successfully! \nYou can find it on the following path: {path}")


def detect_readable_pages_pdfs(pdf_tuple: str) -> dict:
    """
    Main execution point of the program and return the results of the parsing
    """

    # obtaining core inputs from pdf tuple
    pdf_file = pdf_tuple[0]
    pdf_index = pdf_tuple[1]

    logging.info(f"started readability detection for file {pdf_file}")

    output = { # defining output
              "path": pdf_file, 
              "index": pdf_index
            }

    # placeholders for pages in pdf
    readable_pages = []
    non_readable_pages = []
    page_num = 1

    with open(pdf_file, "rb") as infile: # starting detection of readable pages
        try:
            for page in PDFPage.get_pages(infile): # iterating through pdf pages

                if "Font" in page.resources.keys(): # detecting readable pages
                    readable_pages.append(page_num)
                
                else: # detecting non readable pages
                    non_readable_pages.append(page_num)
                
                page_num += 1

            num_pages = page_num - 1 # recording parsing data of current pdf
            output["num_pages"] = num_pages,
            output["readable_pages"] = len(readable_pages),
            output["non_readable_pages"] = len(non_readable_pages),
            output["percentage_readable"] = round(((num_pages - len(non_readable_pages))/num_pages)*100, 2)

        except:  # recording parsing data of current pdf
            output["num_pages"] = "-"
            output["readable_pages"] = "-"
            output["non_readable_pages"] = "-"
            output["percentage_readable"] = "-"

    logging.info(f"completed readability detection for file {pdf_file}")
    return output
 

if __name__ == "__main__":
    logging.info(f"starting the detection of pdfs")
    pdf_paths = glob(str(PDF_DIRECTORY) + "/*.pdf", recursive=True) # reading pdfs

    folder_creator(REPORT_FOLDER, "./") # create folder for detection reports

    # creating a list of pdfs with indexes
    index = 0
    indexed_pdf_paths_raw = [] 
    for pdf in pdf_paths:
        indexed_pdf_paths_raw.append([pdf, index])
        index += 1

    indexed_pdf_paths = indexed_pdf_paths_raw[INITIAL_INDEX: FINAL_INDEX] # keeping only pdfs of our interest

    readability_report = pd.DataFrame() # placeholder for df with final report

    for pdf_tuple in tqdm(indexed_pdf_paths): 
        dict_report_pdf = detect_readable_pages_pdfs(pdf_tuple) # detecting readable pages

        readability_report = readability_report.append(dict_report_pdf, ignore_index=True) # appending reports

        readability_report.to_excel("./" + REPORT_NAME + f"{INITIAL_INDEX}-{FINAL_INDEX}.xlsx", index=False) # storing reports locally
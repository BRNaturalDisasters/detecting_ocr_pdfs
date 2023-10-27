# Import libraries
import pytesseract, platform, json, logging, os, shutil, signal
from pdfminer.pdfpage import PDFPage
from pathlib import Path 
import pandas as pd
from glob import glob
from tqdm import tqdm
from pdf2image import convert_from_path
from PIL import Image
from contextlib import contextmanager


PARSER_PARAMS = "./pdf_parser_params.json"
LOGS_FOLDER = "./logs"
LOGNAME = f"/log_pdf_parser.log"
REPORT_FOLDER = "reports"
REPORT_NAME = REPORT_FOLDER + "/report_parsing_"

pdf_scanner_paths = json.load(open(PARSER_PARAMS)) # opening json file
os.environ["OMP_THREAD_LIMIT"] = "1" # avoids threading overhead within pytesseract

if platform.system() == "Windows":
    pytesseract.pytesseract.tesseract_cmd = (
        pdf_scanner_paths["paths"]["windows_paths"]["tesseract"]
    )
    PATH_TO_POPPLER_EXE = Path(pdf_scanner_paths["paths"]["windows_paths"]["path_to_poppler_exe"])
     
    # Put our output files in a sane place...
    OUT_DIRECTORY = pdf_scanner_paths["paths"]["windows_paths"]["out_directory"]
    PDF_DIRECTORY = pdf_scanner_paths["paths"]["windows_paths"]["pdfs_data"]
    TEMP_PATH = pdf_scanner_paths["paths"]["windows_paths"]["temp_path"]
else:
    OUT_DIRECTORY = pdf_scanner_paths["paths"]["unix_paths"]["out_directory"]
    PDF_DIRECTORY = pdf_scanner_paths["paths"]["unix_paths"]["pdfs_data"]
    TEMP_PATH = pdf_scanner_paths["paths"]["unix_paths"]["temp_path"]

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


class TimeoutException(Exception): 
    """Defining exception to limit time for parsing page"""
    pass


@contextmanager
def time_limit(seconds):
    """
    Defines a limit for a function/operation call
    """

    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)


def folder_creator(folder_name: str, path: str) -> None:
    """
    Generates a folder in specified path
    
    input: name of root folder, path where you want 
    folder to be created
    output: None
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

            num_pages = page_num - 1 
            output["num_pages"] = num_pages,
            output["readable_pages"] = len(readable_pages),
            output["non_readable_pages"] = len(non_readable_pages),
            output["percentage_readable"] = round(((num_pages - len(non_readable_pages))/num_pages)*100, 2)

        except:
            output["num_pages"] = "-"
            output["readable_pages"] = "-"
            output["non_readable_pages"] = "-"
            output["percentage_readable"] = "-"

    logging.info(f"completed readability detection for file {pdf_file}")
    return output

 
def parse_pdf_img(pdf_file: str, text_file: str) -> dict:
    """
    Main execution point of the program and return the results of the parsing
    """

    output = {"file_name": ""}

    logging.info(f"parsing started for file {pdf_file}")
    logging.info("part 1: converting pdf to images")
    if platform.system() == "Windows":
        pdf_pages = convert_from_path(
            pdf_file, 500, poppler_path=PATH_TO_POPPLER_EXE
        )
    else:
        pdf_pages = convert_from_path(pdf_file, 500) # read pdf at 500 DPI

    image_file_list = [] # store all the pages of the pdf in a variable

    pdf_name = pdf_file.split(".pdf")[0].split("/")[-1]
    output["file_name"] = pdf_name
    
    for page_enumeration, page in enumerate(pdf_pages, start=1): # iterate through all the pages stored above
        logging.info("inside loop")
        filename = f"{TEMP_PATH}/page_{page_enumeration:03}_file_{pdf_name}.jpg" # create a file name to store the image
        logging.info(f"storing image in {filename}")
        
        # image preprocessing
        page = page.convert("L") # grayscaling 

        # Save the image of the page in system
        page.save(filename, "JPEG")
        image_file_list.append(filename)
        logging.info("image stored successfully")

    logging.info("part 2: recognizing text from the images using OCR")
    with open(text_file, "a") as output_file: # append mode so all images added to same file
        
        current_page = 1 # iterate from 1 to total number of pages
        output["pages_w_issues"] = [] # pages w issues on parsing
        output["parsing_status"] = "" # placeholder for parsing status
        for image_file in image_file_list:
            try:
                text = pytesseract.image_to_string(Image.open(image_file)) # recognize the text as string in image using pytesserct
                text = text.replace("-\n", "") # formatting the variable
                output_file.write(text) # saving processed text
            except Exception as ex:
                logging.info(f"An exception occurred of type {type(ex).__name__} in page {current_page}")
                output["pages_w_issues"].append(current_page)
            current_page += 1 
        
        output["num_pages"] = current_page # storing total number of pages
        output["percentage_parsed"] = round(((current_page - len(output["pages_w_issues"]))/current_page)*100, 2) # storing total number of pages
       
        # determine status of parsing
        if len(output["pages_w_issues"]) == current_page:
            output["parsing_status"] = "Failed"
        elif len(output["pages_w_issues"]) == 0:
            output["parsing_status"] = "Success"
        else:
            output["parsing_status"] = "Incomplete"

        output["pages_w_issues"] = str(output["pages_w_issues"])
        logging.info(f"\ncompleted parsing of pdf: {pdf_file}")

    for file_path in image_file_list: # erasing the temporary images
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.info('Failed to delete %s. Reason: %s' % (file_path, e))
    logging.info(f"\nerased temp images of current pdf")

    return output


def parsing_multiple_pdfs(list_of_pdfs: list, parsing_function) -> None:
    """
    Parses multiple pdfs 
    """

    pdf_tuples = list_of_pdfs[0] # list of pdf tuples [pdf, index]
    pdf_chunk_index = list_of_pdfs[1]
    first_index = pdf_tuples[0][1]
    final_index = pdf_tuples[-1][1]

    scanned_pdfs_report = pd.DataFrame()

    logging.info(f"pdf chunk {pdf_chunk_index}: parsing started")
    logging.info(f"pdf chunk {pdf_chunk_index}: num pdfs is {len(pdf_tuples)}")
    for pdf_tuple in tqdm(pdf_tuples):

        pdf = pdf_tuple[0] # obtaining pdf and global index
        pdf_index = pdf_tuple[1]

        if platform.system() == "Windows":
            pdf_name = pdf.split(".pdf")[0].split("\\")[-1]
            out_path = OUT_DIRECTORY + "\\" + pdf_name + ".txt"
        else: # assume Unix otherwise
            pdf_name = pdf.split(".pdf")[0].split("/")[-1]
            out_path = OUT_DIRECTORY + "/" + pdf_name + ".txt"
        
        logging.info(f"started parsing of pdf indexed at {pdf_index}")
        dict_report_pdf = parsing_function(pdf, out_path)
        dict_report_pdf["pdf_index"] = pdf_index
        scanned_pdfs_report = scanned_pdfs_report.append(dict_report_pdf, ignore_index=True)
        scanned_pdfs_report.to_excel("./" + REPORT_NAME + f"{first_index}-{final_index}.xlsx", index=False)
        logging.info(f"completed parsing of pdf indexed at {pdf_index}")
        
    logging.info(f"parsing of pdf chunk {pdf_chunk_index} finished")


def divide_chunks(list_input: list, num_chunks: int) -> list:
    """
    Divides list into chucks of size num_chunks
    """
    for index in range(0, len(list_input), num_chunks):
        yield list_input[index:index + num_chunks]


if __name__ == "__main__":
    logging.info(f"starting the detection of pdfs")
    pdf_paths = glob(str(PDF_DIRECTORY) + "/*.pdf", recursive=True) # reading pdfs

    folder_creator(REPORT_FOLDER, "./") # create folder for reports

    indexed_pdf_paths_raw = [] # adding index to pdf
    index = 0
    for pdf in pdf_paths:
        indexed_pdf_paths_raw.append([pdf, index])
        index += 1

    indexed_pdf_paths = indexed_pdf_paths_raw[INITIAL_INDEX: FINAL_INDEX] # keeping only pdfs of our interest

    readability_report = pd.DataFrame()
    for pdf_tuple in tqdm(indexed_pdf_paths):
        dict_report_pdf = detect_readable_pages_pdfs(pdf_tuple)
        readability_report = readability_report.append(dict_report_pdf, ignore_index=True)
        readability_report.to_excel("./" + REPORT_NAME + f"{INITIAL_INDEX}-{FINAL_INDEX}.xlsx", index=False)
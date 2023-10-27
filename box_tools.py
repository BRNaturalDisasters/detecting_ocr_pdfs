from boxsdk import Client, OAuth2
from dotenv import load_dotenv
import os, logging, json, tqdm
import pandas as pd

logging.basicConfig(filename="log_box_tools.log",
                    filemode="a",
                    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S",
                    level=logging.INFO)

parameters_path = "parameters.json"


def box_client_initializer() -> object:
    """
    Initializes Box client 
    """
    
    load_dotenv() # loading env vars
    authenticator = OAuth2(os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET"), access_token=os.getenv("ACCESS_TOKEN"))

    client = Client(authenticator)    
    return client


def list_items_w_url(client_obj: object, folder_url: str) -> dict:
    """
    Returns a dict with all the folder and file ids
    in current folder
    """

    output = {"folders": [], "files": []} # placeholder for project folders/files ids

    folder = client_obj.get_shared_item(folder_url) # obtaining all project folders/files data
    raw_folder_data = folder.get()["item_collection"]["entries"] 
    
    for item in raw_folder_data:
        if item["type"] == "folder": # storing ids of folders/files
            output["folders"].append(item["id"])
        else:
            output["files"].append(item["id"])
    
    return output


def obtain_sample(client_obj: object, folders_list: str, file_format="pdf") -> list:
    """
    Obtains a stratified random sample of files from the chosen folder
    """

    output = {}
    for folder in folders_list:
        file_ids = [] # finding all files in folder
        folder_obj = client_obj.folder(folder_id=folder).get()
        folder_items = folder_obj["item_collection"]["entries"]

        for file in folder_items: # storing ids of all pdf files
            if file["type"] == "file" and file_format in file["name"]: # if pdfs on root of current folder
                file_ids.append(file["id"])

            elif file["type"] == "folder" and "downloadedPDFs" in file["name"]: # if pdfs on folder within current folder
                subfolder_files = client_obj.folder(folder_id=file["id"]).get()["item_collection"]["entries"]
                
                for sub_file in subfolder_files: # storing ids of all pdf files in sub folder
                    if sub_file["type"] == "file" and file_format in sub_file["name"]: 
                        file_ids.append(sub_file["id"])
        
        logging.info(f"current folder id: {folder}, num files: {len(file_ids)}")
        output[f"{folder}"] = file_ids # storing the found pdf ids

    return output


def obtain_store_pdfs(client: object, folder_url: str, pdf_ids_file: str) -> dict:
    """
    Obtains and stores sample of pdf ids locally
    """

    # create sample if not found locally
    if not os.path.isfile(pdf_ids_file):
        folders_and_files = list_items_w_url(client, folder_url)
        sample_pdfs = obtain_sample(client, folders_and_files["folders"])

        with open(pdf_ids_file, "w") as file: # storing ids
            json.dump(sample_pdfs, file)
    
    else: # read and parse file if exists
        sample_pdf_file_obj = open(pdf_ids_file)
        sample_pdfs = json.load(sample_pdf_file_obj)

    return sample_pdfs # return dict w ids


def create_stratified_sample(pdf_ids_file: dict, proportion=0.3) -> tuple:
    """
    Creates a stratified random sample of a size equal to the 
    specified proportion of the whole data
    """

    # from dict to df
    pdf_ids_df = pd.DataFrame([(folder, file) for (folder, file_ids) in pdf_ids_file.items() for file in file_ids], 
                 columns=["folder_id", "file_id"])
    
    # details from original df
    logging.info(f"original dataset size: {len(pdf_ids_df)} | unique folders: {len(pdf_ids_df.folder_id.unique())} | unique files: {len(pdf_ids_df.file_id.unique())}")

    # stratified random sampling
    sample_df = pdf_ids_df.groupby("folder_id", group_keys=False).apply(lambda folder_files: folder_files.sample(frac=proportion)).reset_index()
    
    # details from stratified sample
    logging.info(f"sample size: {len(sample_df)} | unique folders: {len(sample_df.folder_id.unique())} | unique files: {len(sample_df.file_id.unique())}")

    return pdf_ids_df, sample_df


def download_pdfs(sample_pdfs: dict, folder_path: str) -> None:
    """
    Downloading pdfs on specified folder
    """
    
    for folder in tqdm.tqdm(list(sample_pdfs.keys())): # reading ids of current folder
        for file_id in tqdm.tqdm(sample_pdfs[folder]): 
            
            file_content = client.file(file_id).content() # downloading pdf data
            
            with open(f"{folder_path}/{file_id}.pdf", "wb") as f: # storing on specified path
                f.write(file_content)
    

if __name__ == "__main__":
    # reading parameters
    parameters_file = open(parameters_path)
    parameters = json.load(parameters_file)

    # setting base parameters
    folder_url = parameters["folder_url"]
    pdf_ids_file = parameters["pdf_ids_file"]
    pdf_download_folder = parameters["pdf_download_folder"]

    client = box_client_initializer()

    # storing and downloading all available pdfs
    all_pdf_ids = obtain_store_pdfs(client, folder_url, pdf_ids_file)
    download_pdfs(all_pdf_ids, pdf_download_folder) 

    # given the universe size (~8000 pdfs), the following snippets are optional 
    # _, sample_pdf_df = create_stratified_sample(all_pdf_ids) # stratified sampling of pdf ids
    # sample_pdf_df.to_excel("./sample_pdf_df.xlsx") # storing dfs with ids locally

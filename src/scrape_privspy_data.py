from dr_privspy import PrivSpy_Retriever
from utils import return_path_to, write_df_to_path, read_csv_to_df

dr_privspy = PrivSpy_Retriever()

path_source = return_path_to("/data/sources/privacyspy/products/")
path_storage = return_path_to("/data/saved/privacyspy/")

# get all "points" (labelled segments)  based on Privacyspy Repo
# df_points = dr_privspy.extract_point_info_from_products(path_source)
# write_df_to_path(df_points,"".join([path_storage,"privspy_points.csv"]))


df_points = read_csv_to_df("".join([path_storage,"privspy_points.csv"]))

sources = list(df_points.sources.unique())
all_sources = dr_privspy.split_sources(sources)
while True:
    try:
        df_docs = dr_privspy.scrape_source_docs(all_sources)
        break
    except:
        pass


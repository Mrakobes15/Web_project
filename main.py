import keyword_handling as kwd



if __name__ == "__main__":
    db_config = {
        'user': 'j1498375',
        'password': 'b5f!g9Lemsyh',
        'host': 'srv48-h-st.jino.ru',
        'database': 'j1498375_test1'
    }
    kwd.process_keywords_in_articles(db_config,"homepage_proper","keywords","keyword_associations")
    #homepage.insert_json_objects_as_string(mwj.fetch_all_as_json(),"Info")
    #homepage.update_columns_from_json("Info","id", ["title", "abstract"])
    #homepage.print_table_info()

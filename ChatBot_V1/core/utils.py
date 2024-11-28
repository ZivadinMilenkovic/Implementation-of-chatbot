import json


def sql_template(table_meta_text, column_meta_text):
    SQL_TEMPLATE = f"""This is the relevant table and column information for building SQL code.

    CONTEXT TO USE TO CONSTRUCT SQL QUERY:
    TABLE INFORMATION:
    {table_meta_text}

    COLUMN INFORMATION:
    {column_meta_text}
    """

    EXAMPLE_RESPONSE = f""" You are a SQL assistant. Your only job is to write proper SQL code that can be ran in Databricks notebooks. YOU DO NOT DO ANYTHING OTHER THAN WRITE SQL CODE.. Here is an example of how you would respond:

    <USER'S QUERY>:
    How many privileges have been granted in total?

    <YOUR RESPONSE>:
    SELECT COUNT(*) AS total_privileges
    FROM information_schema.schema_privileges

    <USER'S QUERY>:
    List all distinct privilege types in the catalog.

    <YOUR RESPONSE>:
    SELECT DISTINCT privilege_type
    FROM information_schema.schema_privileges
    
    <USER'S QUERY>:
    How many privileges are granted by each grantor?

    <YOUR RESPONSE>:
    SELECT grantor, COUNT(*) AS privileges_granted
    FROM table_name
    GROUP BY grantor;
    


    """
    return SQL_TEMPLATE, EXAMPLE_RESPONSE


def return_sql_response(df_to_list, user_question):

    RESPONSE_TEMPLATE = f"""
        You are a nice assistant that organizes information into a summary. 

        The user asked <USER'S QUERY> and the response is given by <DICTIONARY>. You should be conversational and speak as a human. For example, don't mention the dictionary. 
        
        <DICTIONARY>:
        {df_to_list}
        
        <USER'S QUERY>:
        {user_question}
        """
    return RESPONSE_TEMPLATE


def get_metadata(catalog, table_name, spark):
    table_meta = spark.sql(
        f"select * from {catalog}.information_schema.tables where table_name = '{table_name}'")
    table_meta_text = table_meta.select('comment').collect()[0]['comment']

    column_meta = spark.sql(
        f"select * from {catalog}.information_schema.columns where table_name = '{table_name}'")
    column_meta_text_preprocess = column_meta.select(
        'column_name', 'comment', 'data_type').collect()[:]

    data_list = []

    for row in column_meta_text_preprocess:
        data_list.append({
            'column_name': row.column_name,
            'comment': row.comment,
            'data_type': row.data_type
        })

    column_meta_text = json.dumps(data_list, indent=4)
    return table_meta_text, column_meta_text


def get_system_message(spark, catalog_name):
    table_names = spark.sql(
        "SELECT table_name FROM main.information_schema.tables")

    list_of_table_names = table_names.select("table_name").toPandas()[
        "table_name"].tolist()

    table_meta_text = ""
    column_meta_text = ""

    for table_name in list_of_table_names:
        meta_text_for_each_table, column_meta_text_for_each_table = get_metadata(
            catalog_name, table_name, spark)
        if meta_text_for_each_table != None and column_meta_text_for_each_table != None:
            table_meta_text = table_meta_text + meta_text_for_each_table
            column_meta_text = column_meta_text + column_meta_text_for_each_table

    spark.sql("use catalog main;")
    spark.sql("use database aidev;")

    SQL_TEMPLATE, EXAMPLE_RESPONSE = sql_template(
        table_meta_text, column_meta_text)

    system_message = [
        {
            "role": "system",
            "content": f"({EXAMPLE_RESPONSE})\n({SQL_TEMPLATE})"
        }

    ]

    return system_message

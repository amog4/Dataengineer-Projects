def trim_all_columns(df):

    trim_string = lambda x : x.rstrip() if isinstance(x,str) else x

    return df.applymap(trim_string)


  
def mysql_datatypes(col,df):
    string_datatype = ['object']
    numerical_datatype = ['int64']
    for _ in col:
        if df[_].dtypes in string_datatype:
            df[_] = df[_].astype('str')
        elif df[_].dtypes  in numerical_datatype:
            df[_] = df[_].astype('float64')
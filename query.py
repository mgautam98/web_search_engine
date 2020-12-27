def expression_query(expression):
    return {
        "size" : 10,
        "sort" : [
            { "_score" : {"order" : "desc"}}
        ],
        "query": {
            "match": {
            "body" : expression
            }
        }
    }
import os
import re
import json
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utils.postgress_conn import SingletonSparkConn
from utils.read_conf import (
    PROCESS_TYPE
)


def FindRegexColumn():
    res_dict = {}
    spark = SingletonSparkConn.get()
    df = spark("test_table")
    df.show()
    regex = '(https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|www\\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9]+\\.[^\\s]{2,}|www\\.[a-zA-Z0-9]+\\.[^\\s]{2,})|(\\{(?:[^{}]|())*\\})|(^\\s*(?:\\+?(\\d{1,3}))?[-. (]*(\\d{3})[-. )]*(\\d{3})[-. ]*(\\d{4})(?: *x(\\d+))?\\s*$)'
    cols_to_dop_mix = []

    ########## process databy spark on ram #######
    if (PROCESS_TYPE == "RAM"):
        cols_to_dop_mix = [c for c, v in df.select([
            F.count(F.when(F.col(c).rlike(regex), 1))
            .alias(c) for c in df.columns if df.schema[c].dataType == T.StringType()
            and not re.search('(^(id|ids)_)|(_(id|ids)_)|(_(id|ids)$)', c)
        ]).first().asDict().items() if v]

        for c in df.columns:
            if re.search('(^(id|ids)_)|(_(id|ids)_)|(_(id|ids)$)', c) and \
                            df.schema[c].dataType == T.StringType():
                cols_to_dop_mix.append(c)

    else:
        for c in df.columns:
            if df.schema[c].dataType != T.StringType():
                continue
            if re.search('(^(id|ids)_)|(_(id|ids)_)|(_(id|ids)$)', c):
                cols_to_dop_mix.append(c)
                continue
            if df.filter(F.col(c).rlike(regex)).count():
                cols_to_dop_mix.append(c)

    print("cols that have regex : " , cols_to_dop_mix)


if __name__ == "__main__":
    FindRegexColumn()

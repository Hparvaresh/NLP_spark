from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from collections import Counter
import pyspark.sql.types as T
import pandas as pd
import numpy as np

def find_n_most_important(list_ner, n):
    """get list and return n more accuracy"""
    return [value for value, id in Counter(list_ner).items()][:n]


class NER():
    """_summary_

    Returns:
        spark pipeline.
    """

    def __init__(self):
        self.pipeline = None

    def set_roberta_pipeline(self):
        """set pipeline of ner to Roberta"""

        document_assembler = DocumentAssembler() \
            .setInputCol('input_cols') \
            .setOutputCol('document')

        tokenizer = Tokenizer() \
            .setInputCols(['document']) \
            .setOutputCol('token')

        token_classifier = RoBertaForTokenClassification \
            .load("./models/roberta_classification_tensorflow")\
            .setInputCols(['token', 'document']) \
            .setOutputCol('ner') \
            .setCaseSensitive(True) \
            .setMaxSentenceLength(512)

        # since output column is IOB/IOB2 style, NerConverter can extract entities
        roberta_keywords = NerConverter() \
            .setInputCols(['document', 'token', 'ner'])\
            .setWhiteList(["EVENT", "FAC", "GPE", "LOC", "ORG", "PERSON", "NORP", "LAW"])\
            .setOutputCol('roberta_keywords')

        finisher = Finisher().setInputCols(
            ['roberta_keywords']).setOutputCols('roberta_ner')

        self.pipeline = Pipeline(stages=[document_assembler, tokenizer, token_classifier,
                                         roberta_keywords, finisher])

    def prepare_data(self, spark_df):
        """concate name and description and make input_cols"""
        return spark_df.withColumn('input_cols', F.concat(spark_df.name,F.lit(' '),
                                                          spark_df.description))

    def __call__(self, spark_df):
        self.set_roberta_pipeline()
        important_ner_udf = F.udf(
            find_n_most_important, T.ArrayType(T.StringType()))

        spark_df = self.prepare_data(spark_df)
        spark_df = self.pipeline.fit(spark_df).transform(spark_df)
        return spark_df.withColumn("important_ner",
                       important_ner_udf(F.col('roberta_ner'), F.lit(4)))\
                           .drop('input_cols', 'roberta_ner')

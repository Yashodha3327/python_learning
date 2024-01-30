{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyMw+xTySPKZrqNZZmZOKWHE",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/Yashodha3327/python_learning/blob/main/Pyspark_Assignment_py.ipynb\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql.functions import *\n",
        "from pyspark.sql.types import *\n",
        "\n",
        "spark=SparkSession.builder.master('local').appName('Order_data_analysis').getOrCreate()\n",
        "\n",
        "cat_sch=StructType([StructField('cat_id',IntegerType()),StructField('dept_id',IntegerType()),StructField('cat_name',StringType())])\n",
        "df_categories=spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\categories\\\\part-00000',schema=cat_sch)\n",
        "df_categories.show()\n",
        "df_categories.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_categories.columns]).show()\n",
        "cust_sch=StructType([StructField('cust_id',IntegerType()),StructField('ÇustFname',StringType()),StructField('ÇustLname',StringType()),StructField('phonenum',StringType()),StructField('homephone',StringType()),StructField('Address',StringType()),StructField('City',StringType()),StructField('State',StringType()),StructField('Zipcode',StringType())])\n",
        "df_customers=spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\customers\\\\part-00000',schema=cust_sch)\n",
        "df_customers.show()\n",
        "df_customers.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_customers.columns]).show()\n",
        "dep_sch=StructType([StructField('dept_id',StringType()),StructField('dept_name',StringType())])\n",
        "df_departments = spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\departments\\\\part-00000',schema=dep_sch)\n",
        "df_departments.show()\n",
        "df_departments.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_departments.columns]).show()\n",
        "ord_item_sch=StructType([StructField('item_id',StringType()),StructField('order_id',StringType()),StructField('product_id',StringType()),StructField('quantity',IntegerType()),StructField('sub_total',StringType()),StructField('product_price',StringType())])\n",
        "df_order_items=spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\order_items\\\\part-00000',schema=ord_item_sch)\n",
        "df_order_items.show()\n",
        "df_order_items.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_order_items.columns]).show()\n",
        "order_sch=StructType([StructField('order_id',StringType()),StructField('order_date',StringType()),StructField('cust_id',IntegerType()),StructField('order_status',StringType())])\n",
        "df_orders=spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\orders\\\\part-00000',schema=order_sch)\n",
        "df_orders.show()\n",
        "df_orders.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_orders.columns]).show()\n",
        "prd_sch=StructType([StructField('pid',StringType()),StructField('dept_id',StringType()),StructField('pname',StringType()),StructField('prd_col',StringType()),StructField('prd_price',StringType()),StructField('plink',StringType())])\n",
        "df_products=spark.read.csv('C:\\\\Users\\\\saranyap\\\\PycharmProjects\\\\pythonProject\\\\Spark_Assignment\\\\Order_Data_Analysis\\\\retail_db\\\\products\\\\part-00000',schema=prd_sch)\n",
        "df_products.show()\n",
        "df_products.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df_products.columns]).show()\n",
        "df_products=df_products.drop('prd_col')\n",
        "df_products.show()\n",
        "#df_orders.filter(lower(col('order_status')).isin('complete','closed')).groupBy('order_status').agg(count('order_status').alias('num_of_orders_by_status')).show()\n",
        "#df=df_orders.join(df_order_items,df_orders.order_id==df_order_items.order_id).join(df_products,df_order_items.product_id==df_products.pid)\n",
        "#df.groupBy('order_date','product_id','pname').filter(lower(col('order_status')).isin('complete','closed')).show()\n",
        "df_products.createOrReplaceTempView(\"products\")\n",
        "df_orders.createOrReplaceTempView('orders')\n",
        "df_order_items.createOrReplaceTempView('order_items')\n",
        "df_daily=spark.sql(\"SELECT o.order_date,oi.product_id,p.pname,sum(oi.sub_total) AS daily_product_revenue FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN products p ON p.pid = oi.product_id WHERE o.order_status IN ('COMPLETE', 'CLOSED') GROUP BY o.order_date, oi.product_id,p.pname\")\n",
        "df_daily.show()\n",
        "df_daily.write.format('parquet').saveAsTable('product_revenue')\n",
        "dfd=spark.read.table('product_revenue')\n",
        "dfd.show()"
      ],
      "metadata": {
        "id": "xepUWOS3T1O1"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}
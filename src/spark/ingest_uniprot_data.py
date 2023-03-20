import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from neo4j_utils import Neo4JConnection, write_nodes_to_neo4j, write_relationship_to_neo4j

spark = SparkSession.builder.getOrCreate()

def read_xml_tag(file_path, row_tag):
    return (
        spark.read.format("xml")
            .option("rowTag", row_tag)
            .option("valueTag", True)
            .load(file_path)
    )

def add_protein_id_column(df):
    return (
        df.withColumn("_src_file", F.input_file_name())
        .withColumn("protein_id", F.regexp_extract("_src_file", "\w*(?=\.xml$)", 0))
        .drop("_src_file")
    )

def load_protein_data(file_path:str, neo4j_conn:Neo4JConnection):
    # LOAD PROTEIN
    df_protein = read_xml_tag(file_path, "protein")
    df_protein = (
        add_protein_id_column(df_protein)
            .withColumnRenamed("protein_id", "id")
            .withColumn("name", F.col("recommendedName.fullName"))
            .select("id", "name")
    )
    df_protein_node = df_protein.select("id")
    write_nodes_to_neo4j(df_protein_node, ":Protein", "id", neo4j_conn)

    # LOAD FULLNAME
    df_protein_fullname_node = df_protein.select("name")
    write_nodes_to_neo4j(df_protein_fullname_node, ":FullName", "name", neo4j_conn)

    # RELATIONSHIP PROTEIN X FULLNAME
    write_relationship_to_neo4j(
        df=df_protein,
        relationship="HAS_FULL_NAME",
        source_labels=":Protein",
        source_node_keys="id:id",
        target_labels=":FullName",
        neo4j_conn=neo4j_conn,
        target_node_keys="name:name"
    )

def load_organism_data(file_path:str, neo4j_conn:Neo4JConnection):
    # LOAD ORGANISM
    df_organism = read_xml_tag(file_path, "organism")
    scientific_name_filter = lambda s: s._type == "scientific"
    common_name_filter = lambda s: s._type == "common"

    df_organism = (
        df_organism.withColumn("taxonomy_id", F.col("dbReference._id"))
        .withColumn("scientific_name", F.element_at(F.filter(F.col("name"), scientific_name_filter), 1))
        .withColumn("scientific_name", F.col("scientific_name.true"))
        .withColumn("common_name", F.element_at(F.filter(F.col("name"), common_name_filter), 1))
        .withColumn("common_name", F.col("common_name.true"))
        .select("taxonomy_id", "scientific_name", "common_name")
    )
    write_nodes_to_neo4j(df_organism, ":Organism", "taxonomy_id", neo4j_conn)

    # RELATIONSHIP PROTEIN X ORGANISM
    write_relationship_to_neo4j(
        df=add_protein_id_column(df_organism),
        relationship="IN_ORGANISM",
        source_labels=":Protein",
        source_node_keys="protein_id:id",
        target_labels=":Organism",
        neo4j_conn=neo4j_conn,
        target_node_keys="taxonomy_id:taxonomy_id"
    )

def load_gene_data(file_path:str, neo4j_conn:Neo4JConnection):
    # LOAD GENE
    df_gene = read_xml_tag(file_path, "gene")
    df_gene = (
        add_protein_id_column(df_gene)
            .withColumn("name", F.explode("name"))
            .withColumn("type", F.col("name._type"))
            .withColumn("name", F.col("name.true"))
    )

    df_gene_node = df_gene.select("name")
    write_nodes_to_neo4j(df_gene_node, ":Gene", "name", neo4j_conn)

    # RELATIONSHIP PROTEIN X GENE
    write_relationship_to_neo4j(
        df=df_gene,
        relationship="FROM_GENE",
        source_labels=":Protein",
        source_node_keys="protein_id:id",
        target_labels=":Gene",
        target_node_keys="name:name",
        neo4j_conn=neo4j_conn,
        relationship_properties="type:status"
    )

def load_feature_data(file_path:str, neo4j_conn:Neo4JConnection):
    # LOAD FEATURE
    df_feature = (
        read_xml_tag(file_path, "feature")
        .withColumnRenamed("_id", "name")
        .filter("name is not null")
        .withColumnRenamed("_type", "type")
    )

    df_feature_node_df = df_feature.select("name", "type")
    write_nodes_to_neo4j(df_feature_node_df, ":Feature", "name", neo4j_conn)

    # RELATIONSHIP PROTEIN X FEATURE
    df_feature_protein = (
        add_protein_id_column(df_feature)
        .withColumn("position", F.col("location.position._position"))
        .select("name", "protein_id", "position")
    )

    write_relationship_to_neo4j(
        df=df_feature_protein,
        relationship="HAS_FEATURE",
        source_labels=":Protein",
        source_node_keys="protein_id:id",
        target_labels=":Feature",
        target_node_keys="name:name",
        relationship_properties="position:position",
        neo4j_conn=neo4j_conn
    )

def load_reference_data(file_path:str, neo4j_conn:Neo4JConnection):
    # LOAD REFERENCES
    df_reference = read_xml_tag(file_path, "reference")
    df_reference = (
        add_protein_id_column(df_reference)
        .withColumn("name", F.col("citation._name"))
        .withColumn("type", F.col("citation._type"))
        .withColumn("id", F.concat(F.col("protein_id"), F.lit("_"), F.col("_key")))
    )
    df_reference_node = df_reference.select("id", "name", "type")
    write_nodes_to_neo4j(df_reference_node, ":Reference", "id", neo4j_conn)


    # RELATIONSHIP PROTEIN X REFERENCES
    write_relationship_to_neo4j(
        df=df_reference,
        relationship="HAS_REFERENCE",
        source_labels=":Protein",
        source_node_keys="protein_id:id",
        target_labels=":Reference",
        target_node_keys="id:id",
        neo4j_conn=neo4j_conn
    )

    # LOAD AUTHORS
    df_authors = (
        df_reference.withColumn("author", F.explode("citation.authorList.person"))
        .withColumn("name", F.col("author._name"))
    )
    df_authors_node = df_authors.select("name")
    write_nodes_to_neo4j(df_authors_node, ":Author", "name", neo4j_conn)

    # RELATIONSHIP REFERENCE X AUTHOR
    write_relationship_to_neo4j(
        df=df_authors,
        relationship="HAS_AUTHOR",
        source_labels=":Reference",
        source_node_keys="id:id",
        target_labels=":Author",
        target_node_keys="name:name",
        neo4j_conn=neo4j_conn
    )


if __name__ == '__main__':
    entity_name = sys.argv[1]
    file_path = sys.argv[2]
    neo4j_url = sys.argv[3]
    neo4j_user = sys.argv[4]
    neo4j_pass = sys.argv[5]

    neo4j_conn = Neo4JConnection(url=neo4j_url, username=neo4j_user, password=neo4j_pass)

    if entity_name == 'protein':
        load_protein_data(file_path, neo4j_conn)
    elif entity_name == 'gene':
        load_gene_data(file_path, neo4j_conn)
    elif entity_name == 'organism':
        load_organism_data(file_path, neo4j_conn)
    elif entity_name == 'feature':
        load_feature_data(file_path, neo4j_conn)
    elif entity_name == 'reference':
        load_reference_data(file_path, neo4j_conn)


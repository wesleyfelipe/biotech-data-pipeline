class Neo4JConnection:

    def __init__(self, username:str, password:str, url:str):
        self.username = username
        self.password = password
        self.url = url


def write_nodes_to_neo4j(df, labels, keys, neo4j_conn: Neo4JConnection):

    (
        df
            .write
            .format("org.neo4j.spark.DataSource")
            .mode("overwrite")
            .option("url", neo4j_conn.url)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", neo4j_conn.username)
            .option("authentication.basic.password", neo4j_conn.password)
            .option("labels", labels)
            .option("node.keys", keys)
            .save()
    )

def write_relationship_to_neo4j(
        df,
        relationship,
        source_labels,
        source_node_keys,
        target_labels,
        target_node_keys,
        neo4j_conn: Neo4JConnection,
        relationship_properties = None,
):
    (
        df.write
            .format("org.neo4j.spark.DataSource")
            .mode("overwrite")
            .option("url", neo4j_conn.url)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", neo4j_conn.username)
            .option("authentication.basic.password", neo4j_conn.password)
            .option("relationship", relationship)
            .option("relationship.properties", relationship_properties)
            .option("relationship.save.strategy", "keys")
            .option("relationship.source.labels", source_labels)
            .option("relationship.source.node.keys", source_node_keys)
            .option("relationship.target.labels", target_labels)
            .option("relationship.target.node.keys", target_node_keys)
            .save()
    )
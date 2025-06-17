from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# Set Headers
reqHeaders = {
    'Accept-Encoding': 'gzip',
    'x-hasura-admin-secret': <myadminsecretkey>
}


# Port 8888 is being forwarded to the internal service that points to hasura's pod
transport = AIOHTTPTransport(url="http://localhost:8888/v1/graphql",headers=reqHeaders)

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# Provide the GraphQL query
query = gql(
"""
query MyQuery {
  table_name {
    column_1
    column_2
    column_3
  }
}
"""
)

# Execute the query on the transport
result = client.execute(query)
print(result)

# Hasura GraphQL Tooling Demo

This repo contains select portions of hasura_tooling to demo its functionalities for developing and testing API schema and metadata.

### Table of Contents

- /hasura_tooling/: lib portion of hasura_tooling, includes helper functions.
- /hasura_tooling_cli/: bin portion of hasura_tooling with the CLI interface.
- /others/: various miscellaneous purpose-specific scripts for quick-checks, and one-off tasks.

### Hasura Tooling Overview

Hasura_tooling is a custom-built tooling that incorporates Hasura's SDK and extends its functionalities to automate common operations. See "components" section in hasura_tooling_cli's README for details on the list of facilitated common operations.

### Hasura Background Overview

Hasura is a graphql API that offers a SQL-like graphql query language that enables:
    - the flexibility of returning data of all columns of an API object (like `SELECT *`) or only specified columns for faster response times,
    - SQL-like joins of multiple API objects via "relationships", and 
    - API access control with a hierarchy of "roles" (conceptually equivalent to organizations), each containing "users" that can have more fine-grained permissions controls enforced.

Hasura's graphql API metadata defines each object's schema, each role's permissions, and queryable relationships between objects.

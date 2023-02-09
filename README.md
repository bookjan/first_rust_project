# first_rust_project

This project is used to learn how to start a GraphQL server by using Rust.

1. Start a PostgreSQL Database
```shell
docker run --rm -it -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:alpine
```

2. Run the GraphQL server
```shell
cd {project_directory}
cargo run
```

3. Open browser and type http://localhost:8080/graphiql to start using the query and mutation features.

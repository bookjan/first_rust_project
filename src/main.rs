use juniper::{graphql_object, EmptySubscription, RootNode};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use warp::{http::Response, Filter};

#[derive(Debug)]
struct AppState {
    client: Client,
}

struct Context {
    app_state: Arc<AppState>,
}

impl juniper::Context for Context {}

#[derive(juniper::GraphQLObject)]
struct Customer {
    id: String,
    name: String,
    age: i32,
    email: String,
    address: String,
}

struct Query;
struct Mutation;

#[graphql_object(Context = Context)]
impl Query {
    async fn customer(ctx: &Context, id: String) -> juniper::FieldResult<Customer> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let row = ctx
            .app_state
            .client
            .query_one(
                "SELECT name, age, email, address FROM customers WHERE id = $1",
                &[&uuid],
            )
            .await?;
        let customer = Customer {
            id,
            name: row.try_get(0)?,
            age: row.try_get(1)?,
            email: row.try_get(2)?,
            address: row.try_get(3)?,
        };
        Ok(customer)
    }

    async fn customers(ctx: &Context) -> juniper::FieldResult<Vec<Customer>> {
        let rows = ctx
            .app_state
            .client
            .query("SELECT id, name, age, email, address FROM customers", &[])
            .await?;
        let mut customers = Vec::new();
        for row in rows {
            let id: uuid::Uuid = row.try_get(0)?;
            let customer = Customer {
                id: id.to_string(),
                name: row.try_get(1)?,
                age: row.try_get(2)?,
                email: row.try_get(3)?,
                address: row.try_get(4)?,
            };
            customers.push(customer);
        }
        Ok(customers)
    }
}

#[graphql_object(Context = Context)]
impl Mutation {
    async fn register_customer(
        ctx: &Context,
        name: String,
        age: i32,
        email: String,
        address: String,
    ) -> juniper::FieldResult<Customer> {
        let id = uuid::Uuid::new_v4();
        let email = email.to_lowercase();
        ctx.app_state
            .client
            .execute(
                "INSERT INTO customers (id, name, age, email, address) VALUES ($1, $2, $3, $4, $5)",
                &[&id, &name, &age, &email, &address],
            )
            .await?;
        Ok(Customer {
            id: id.to_string(),
            name,
            age,
            email,
            address,
        })
    }

    async fn update_customer_email(
        ctx: &Context,
        id: String,
        email: String,
    ) -> juniper::FieldResult<String> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let email = email.to_lowercase();
        let n = ctx
            .app_state
            .client
            .execute(
                "UPDATE customers SET email = $1 WHERE id = $2",
                &[&email, &uuid],
            )
            .await?;
        if n == 0 {
            return Err("User does not exist".into());
        }
        Ok(email)
    }

    async fn delete_customer(ctx: &Context, id: String) -> juniper::FieldResult<bool> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let n = ctx
            .app_state
            .client
            .execute("DELETE FROM customers WHERE id = $1", &[&uuid])
            .await?;
        if n == 0 {
            return Err("User does not exist".into());
        }
        Ok(true)
    }
}

type Schema = RootNode<'static, Query, Mutation, EmptySubscription<Context>>;

fn schema() -> Schema {
    Schema::new(
        Query,
        Mutation,
        EmptySubscription::<Context>::new(),
    )
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "warp_async");
    env_logger::init();

    let log = warp::log("warp_server");

    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password='postgres'", NoTls)
            .await
            .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS customers(
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            age INT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            address TEXT NOT NULL
        )",
            &[],
        )
        .await
        .expect("Could not create table");

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/graphiql\">/graphiql</a></html>",
            )
    });

    log::info!("Listening on 127.0.0.1:8080");

    let app_state = Arc::new(AppState { client: client });
    let app_state = warp::any().map(move || app_state.clone());

    let state = warp::any()
        .and(app_state)
        .map(|app_state: Arc<AppState>| Context {
            app_state: app_state,
        });
    let graphql_filter = juniper_warp::make_graphql_filter(schema(), state.boxed());

    warp::serve(
        warp::get()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_filter("/graphql", None))
            .or(homepage)
            .or(warp::path("graphql").and(graphql_filter))
            .with(log),
    )
    .run(([127, 0, 0, 1], 8080))
    .await
}

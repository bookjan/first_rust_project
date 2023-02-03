use std::{env, pin::Pin, sync::Arc, time::Duration};

use futures::{FutureExt as _, Stream};
use juniper::{
    graphql_object, graphql_subscription, graphql_value, EmptyMutation, FieldError, GraphQLEnum,
    RootNode,
};
use juniper_graphql_ws::ConnectionConfig;
use juniper_warp::{playground_filter, subscriptions::serve_graphql_ws};
use warp::{http::Response, Filter};
use tokio_postgres::{Client, NoTls};

struct Context {
    dbClient: Client,
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
struct Subscription;

#[graphql_object(Context = Context)]
impl Query {
    async fn customer(ctx: &Context, id: String) -> juniper::FieldResult<Customer> {
        let uuid = uuid::Uuid::parse_str(&id)?;
        let row = ctx
            .dbClient
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
            .dbClient
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
        ctx.dbClient
            .execute(
                "INSERT INTO customers (id, name, age, email, address) VALUES ($1, $2, $3, $4, $5)",
                &[&id, &name, &age.to_string(), &email, &address],
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
            .dbClient
            .execute("DELETE FROM customers WHERE id = $1", &[&uuid])
            .await?;
        if n == 0 {
            return Err("User does not exist".into());
        }
        Ok(true)
    }
}

type CustomerStream = Pin<Box<dyn Stream<Item = Result<Customer, FieldError>> + Send>>;



#[graphql_subscription(context = Context)]
impl Subscription {
    async fn customers() -> CustomerStream {
        let mut counter = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let stream = async_stream::stream! {
            counter += 1;
            loop {
                interval.tick().await;
                if counter == 2 {
                    yield Err(FieldError::new(
                        "some field error from handler",
                        graphql_value!("some additional string"),
                    ))
                } else {
                    yield Ok(Customer {
                        id: counter.to_string(),
                        name: counter.to_string(),
                        age: counter,
                        email: counter.to_string(),
                        address: counter.to_string(),
                    })
                }
            }
        };

        Box::pin(stream)
    }
}

type Schema = RootNode<'static, Query, EmptyMutation<Context>, Subscription>;

fn schema() -> Schema {
    Schema::new(Query, EmptyMutation::new(), Subscription)
}

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "warp_subscriptions");
    env_logger::init();

    let log = warp::log("warp_subscriptions");

    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres password='postgres'", NoTls)
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
            .body("<html><h1>juniper_subscriptions demo</h1><div>visit <a href=\"/playground\">graphql playground</a></html>")
    });

    let qm_schema = schema();
    let qm_state = warp::any().map(|| Context);
    let qm_graphql_filter = juniper_warp::make_graphql_filter(qm_schema, qm_state.boxed());

    let root_node = Arc::new(schema());

    log::info!("Listening on 127.0.0.1:8080");

    let routes = (warp::path("subscriptions")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let root_node = root_node.clone();
            ws.on_upgrade(move |websocket| async move {
                serve_graphql_ws(websocket, root_node, ConnectionConfig::new(Context))
                    .map(|r| {
                        if let Err(e) = r {
                            println!("Websocket error: {e}");
                        }
                    })
                    .await
            })
        }))
    .map(|reply| {
        // TODO#584: remove this workaround
        warp::reply::with_header(reply, "Sec-WebSocket-Protocol", "graphql-ws")
    })
    .or(warp::post()
        .and(warp::path("graphql"))
        .and(qm_graphql_filter))
    .or(warp::get()
        .and(warp::path("playground"))
        .and(playground_filter("/graphql", Some("/subscriptions"))))
    .or(homepage)
    .with(log);

    warp::serve(routes).run(([127, 0, 0, 1], 8080)).await;
}
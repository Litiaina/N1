use axum::response::Html;
use axum::{http::StatusCode, response::IntoResponse};

pub async fn handler_404() -> impl IntoResponse {
    let html = r#"
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>404 Not Found</title>
    <style>
      * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
      }
      h1 {
        font-size: 5rem;
        margin-bottom: 10px;
        color: rgb(133, 23, 23);
      }
      p {
        font-size: 0.9rem;
        color: #cccbcb;
        margin-bottom: 20px;
      }
      button {
        padding: 10px 20px;
        background: #222;
        color: #fff;
        border: none;
        border-radius: 8px;
        cursor: pointer;
      }
      button:hover {
        background: #333;
      }
      .main-container {
        background-color: #000;
        color: #fff;
        font-family: "Segoe UI", sans-serif;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        min-height: 100vh;
        text-align: center;
        padding: 20px;
      }
      .card-container {
        display: flex;
        flex-direction: column;
        width: 800px;
        height: 400px;
        background-color: #212121;
        border-radius: 10px 10px 10px 10px;
        /* box-shadow: 1px 6px 25px -7px rgba(201, 201, 201, 0.54);
        -webkit-box-shadow: 1px 6px 25px -7px rgba(201, 201, 201, 0.54);
        -moz-box-shadow: 1px 6px 25px -7px rgba(201, 201, 201, 0.54); */
      }
      .header {
        height: 30px;
        background-color: #791919;
        border-radius: 10px 10px 0px 0px;
        align-items: center;
        padding: 10px;
        display: flex;
        gap: 10px;
      }
      .body {
        position: relative;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        height: 100%;
        width: 100%;
      }
      .main-text-container {
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
      }
      .circle {
        height: 10px;
        width: 10px;
        border-radius: 50%;
        background-color: rgb(255, 255, 255);
      }
      .aesthethics {
        color: green;
        font-weight: 200;
        text-align: left;
        position: absolute;
        font-size: 12px;
        left: 10px;
        top: 10px;
      }
      .sad-face {
        position: relative;
        width: 100px;
        height: 100px;
        background-color: #be828200;
        border-radius: 50%;
        border: 2px solid #8b0000;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
      }

      .eyes-container {
        display: flex;
        justify-content: space-between;
        width: 60%;
        position: absolute;
        top: 30%;
      }

      .eye {
        width: 15px;
        height: 15px;
        background-color: #ad2121;
        border-radius: 50%;
      }

      .mouth {
        position: absolute;
        bottom: 20%;
        width: 50px;
        height: 20px;
        border: 2px solid #8b0000;
        border-bottom: none;
        border-radius: 50px 50px 0px 0px;
      }
    </style>
  </head>
  <body>
    <!-- main container -->
    <div class="main-container">
      <!-- card container -->
      <div class="card-container">
        <!-- header -->
        <div class="header">
          <!-- three circles cause why not -->
          <div class="circle"></div>
          <div class="circle"></div>
          <div class="circle"></div>
        </div>
        <!-- body -->
        <div class="body">
          <div class="aesthethics">
            <div>Err....... not Found</div>
          </div>
          <!-- main text -->
          <div class="main-text-container">
            <div class="sad-face">
              <div class="eyes-container">
                <div class="eye"></div>
                <div class="eye"></div>
              </div>
              <div class="mouth"></div>
            </div>
            <h1>404</h1>
            <p>Oops, the page you're looking for doesn't exist</p>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
"#;

    (StatusCode::NOT_FOUND, Html(html))
}

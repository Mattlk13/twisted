function doConnect() {
 webSocket = new WebSocket("ws://localhost:8080/webskt");
 webSocket.onopen = (event) => {
  webSocket.send("hello world");
  alert("connection opened");
 }

 /* be utf-8 please */
 webSocket.onmessage = (event) => {
  alert("message received: «" + event.data + "»");
 }
 /* pleeease */

}


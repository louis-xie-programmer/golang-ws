// 创建WebSocket连接
const socket = new WebSocket('ws://localhost:8080/ws');

// 连接打开时触发的事件
socket.onopen = function(event) {
    socket.send("join:roomA");
    socket.send("task:hello");
};

// 接收到消息时触发的事件
socket.onmessage = function(e) {
    console.log("recv:", e.data);
};

// 连接关闭时触发的事件
socket.onclose = function(event) {
    console.log('连接已关闭');
};

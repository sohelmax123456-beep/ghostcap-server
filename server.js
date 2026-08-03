const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// Health-check endpoint for UptimeRobot
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('GhostCap Server is Running 24/7!');
});

const wss = new WebSocket.Server({ server });
const rooms = {};

wss.on('connection', (ws) => {
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            
            switch (data.type) {
                case 'create_room': {
                    const roomId = Math.floor(100000 + Math.random() * 900000).toString();
                    rooms[roomId] = { host: ws, controller: null };
                    ws.roomId = roomId;
                    ws.role = 'host';
                    ws.send(JSON.stringify({ type: 'room_created', roomId }));
                    break;
                }
                case 'join_room': {
                    const room = rooms[data.roomId];
                    if (room && !room.controller) {
                        room.controller = ws;
                        ws.roomId = data.roomId;
                        ws.role = 'controller';
                        ws.send(JSON.stringify({ type: 'joined_successfully', roomId: data.roomId }));
                        if (room.host && room.host.readyState === WebSocket.OPEN) {
                            room.host.send(JSON.stringify({ type: 'peer_joined' }));
                        }
                    } else {
                        ws.send(JSON.stringify({ type: 'error', message: 'Invalid Room ID' }));
                    }
                    break;
                }
                case 'offer':
                case 'answer':
                case 'candidate': {
                    const room = rooms[ws.roomId];
                    if (room) {
                        const target = (ws.role === 'host') ? room.controller : room.host;
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify(data));
                        }
                    }
                    break;
                }
            }
        } catch (e) {
            console.error('Error:', e.message);
        }
    });

    ws.on('close', () => {
        if (ws.roomId && rooms[ws.roomId]) {
            delete rooms[ws.roomId];
        }
    });
});

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

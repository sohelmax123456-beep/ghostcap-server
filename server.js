
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 10000;

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('GhostCap Signaling Server Active');
});

const wss = new WebSocket.Server({ server });

// Map: roomId -> { host: socket, receiver: socket }
const rooms = new Map();

wss.on('connection', (ws) => {
    console.log('Client connected');
    let currentRoomId = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            const { type, roomId } = msg;

            switch (type) {
                // 1. Explicit Host Registration
                case 'create_room':
                case 'identify_host': {
                    if (!roomId) return;
                    currentRoomId = roomId;

                    let room = rooms.get(roomId) || { host: null, receiver: null };
                    room.host = ws;
                    rooms.set(roomId, room);

                    console.log(`[Room ${roomId}] Host registered explicitly`);
                    ws.send(JSON.stringify({ type: 'room_created', roomId }));

                    // Agar receiver pehle se wait kar raha tha, toh host ko trigger do
                    if (room.receiver && room.receiver.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'peer_joined', roomId }));
                    }
                    break;
                }

                // 2. Explicit Client/Receiver Registration
                case 'join_room':
                case 'identify_client': {
                    if (!roomId) return;
                    currentRoomId = roomId;

                    let room = rooms.get(roomId) || { host: null, receiver: null };
                    room.receiver = ws;
                    rooms.set(roomId, room);

                    console.log(`[Room ${roomId}] Receiver registered explicitly`);
                    ws.send(JSON.stringify({ type: 'joined_successfully', roomId }));

                    // Host ko inform karo ki Peer aagaya hai (Offer generate karne ke liye)
                    if (room.host && room.host.readyState === WebSocket.OPEN) {
                        console.log(`[Room ${roomId}] Notifying Host to start SDP exchange`);
                        room.host.send(JSON.stringify({ type: 'peer_joined', roomId }));
                    }
                    break;
                }

                // 3. WebRTC Relay (Offer / Answer / ICE Candidates)
                case 'offer':
                case 'answer':
                case 'candidate': {
                    if (currentRoomId && rooms.has(currentRoomId)) {
                        const room = rooms.get(currentRoomId);
                        const target = (ws === room.host) ? room.receiver : room.host;

                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify(msg));
                        }
                    }
                    break;
                }
            }
        } catch (e) {
            console.error('Signaling error:', e);
        }
    });

    ws.on('close', () => {
        if (currentRoomId && rooms.has(currentRoomId)) {
            const room = rooms.get(currentRoomId);

            if (ws === room.host) {
                console.log(`[Room ${currentRoomId}] Host disconnected.`);
                room.host = null;
                if (room.receiver && room.receiver.readyState === WebSocket.OPEN) {
                    room.receiver.send(JSON.stringify({ type: 'error', message: 'Host disconnected' }));
                }
            } else if (ws === room.receiver) {
                console.log(`[Room ${currentRoomId}] Receiver disconnected.`);
                room.receiver = null;
                if (room.host && room.host.readyState === WebSocket.OPEN) {
                    room.host.send(JSON.stringify({ type: 'peer_disconnected', message: 'Receiver left' }));
                }
            }

            // Room cleanup jab dono disconnect ho jayein
            if (!room.host && !room.receiver) {
                rooms.delete(currentRoomId);
                console.log(`[Room ${currentRoomId}] Deleted empty room.`);
            }
        }
        console.log('Client disconnected');
    });

    ws.on('error', (err) => console.error('WebSocket Error:', err));
});

server.listen(PORT, () => {
    console.log(`GhostCap Server running on port ${PORT}`);
});

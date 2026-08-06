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
                case 'create_room':
                case 'join_room':
                case 'identify_host':
                case 'identify_client':
                    if (!roomId) return;

                    // 1. Cleanup: If this socket was already in a room, remove it
                    if (currentRoomId && rooms.has(currentRoomId)) {
                        const r = rooms.get(currentRoomId);
                        if (r.host === ws) r.host = null;
                        if (r.receiver === ws) r.receiver = null;
                        if (!r.host && !r.receiver) rooms.delete(currentRoomId);
                    }

                    currentRoomId = roomId;

                    if (!rooms.has(roomId)) {
                        // 2. Assign HOST: First client to join becomes the Host
                        rooms.set(roomId, { host: ws, receiver: null });
                        console.log(`[Room ${roomId}] Host registered`);
                        
                        // Send success confirmation matching SignalingClient expectations
                        const response = (type === 'create_room' || type === 'identify_host') 
                            ? 'room_created' : 'joined_successfully';
                        ws.send(JSON.stringify({ type: response, roomId }));
                    } else {
                        const room = rooms.get(roomId);
                        
                        // Duplicate socket protection
                        if (room.host === ws || room.receiver === ws) return;

                        if (!room.receiver) {
                            // 3. Assign RECEIVER: Second client to join becomes the Receiver
                            room.receiver = ws;
                            console.log(`[Room ${roomId}] Receiver registered`);
                            
                            ws.send(JSON.stringify({ type: 'joined_successfully', roomId }));
                            
                            // 4. Trigger Handshake: ONLY notify Host that Peer Joined
                            if (room.host && room.host.readyState === WebSocket.OPEN) {
                                console.log(`[Room ${roomId}] Notifying Host to start capture`);
                                room.host.send(JSON.stringify({ type: 'peer_joined', roomId }));
                            }
                        } else {
                            // 5. Duplicate room protection: Prevent 3rd wheel
                            console.log(`[Room ${roomId}] Connection rejected: Room full`);
                            ws.send(JSON.stringify({ type: 'error', message: 'Room already full' }));
                        }
                    }
                    break;

                case 'offer':
                case 'answer':
                case 'candidate':
                    // RELAY: Logic for relaying WebRTC data between Host and Receiver
                    if (currentRoomId && rooms.has(currentRoomId)) {
                        const room = rooms.get(currentRoomId);
                        const target = (ws === room.host) ? room.receiver : room.host;
                        
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify(msg));
                        }
                    }
                    break;
            }
        } catch (e) {
            console.error('Signaling relay error:', e);
        }
    });

    ws.on('close', () => {
        if (currentRoomId && rooms.has(currentRoomId)) {
            const room = rooms.get(currentRoomId);
            if (ws === room.host) {
                console.log(`[Room ${currentRoomId}] Host left. Closing room.`);
                // Notify Receiver if Host leaves
                if (room.receiver && room.receiver.readyState === WebSocket.OPEN) {
                    room.receiver.send(JSON.stringify({ type: 'error', message: 'Host disconnected' }));
                }
                rooms.delete(currentRoomId);
            } else if (ws === room.receiver) {
                console.log(`[Room ${currentRoomId}] Receiver left.`);
                room.receiver = null;
            }
        }
        console.log('Client disconnected');
    });

    ws.on('error', (err) => console.error('WebSocket Error:', err));
});

server.listen(PORT, () => {
    console.log(`GhostCap Server running on port ${PORT}`);
});

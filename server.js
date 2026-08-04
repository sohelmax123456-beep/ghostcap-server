
const WebSocket = require('ws');
const admin = require('firebase-admin');

// Render Environment Variable se Firebase Admin load karo
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    try {
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount)
        });
        console.log("Firebase Admin Initialized Successfully!");
    } catch (e) {
        console.error("Firebase Initialization Failed:", e);
    }
} else {
    console.warn("FIREBASE_SERVICE_ACCOUNT environment variable missing!");
}

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = {};

wss.on('connection', (ws) => {
    console.log('New Client Connected');

    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);

            // 1. Host Room Create Karta Hai
            if (data.type === 'create_room') {
                rooms[data.roomId] = { host: ws, receiver: null };
                ws.roomId = data.roomId;
                ws.isHost = true;
                console.log(`Room created: ${data.roomId}`);
            } 
            // 2. Controller Join Karta Hai
            else if (data.type === 'join_room') {
                const room = rooms[data.roomId];
                
                // Agar Host offline hai, toh FCM Silent Push bhej kar jagao
                if (!room || !room.host) {
                    console.log(`Host offline for room ${data.roomId}. Sending Silent FCM Wake-Up...`);
                    
                    if (admin.apps.length > 0) {
                        const payload = {
                            data: {
                                action: "WAKE_UP",
                                roomId: data.roomId
                            },
                            topic: `room_${data.roomId}`
                        };

                        try {
                            await admin.messaging().send(payload);
                            console.log("FCM Silent Notification Sent!");
                        } catch (error) {
                            console.error("FCM Send Error:", error);
                        }
                    }
                } else {
                    room.receiver = ws;
                    ws.roomId = data.roomId;
                    ws.isHost = false;
                    console.log(`Receiver joined room: ${data.roomId}`);
                }
            } 
            // 3. WebRTC Signaling Relaying (Offer, Answer, Candidates)
            else {
                const room = rooms[data.roomId];
                if (room) {
                    const target = ws.isHost ? room.receiver : room.host;
                    if (target && target.readyState === WebSocket.OPEN) {
                        target.send(JSON.stringify(data));
                    }
                }
            }
        } catch (err) {
            console.error("Message processing error:", err);
        }
    });

    ws.on('close', () => {
        if (ws.roomId && rooms[ws.roomId]) {
            if (ws.isHost) {
                delete rooms[ws.roomId];
            } else {
                rooms[ws.roomId].receiver = null;
            }
        }
        console.log('Client Disconnected');
    });
});

console.log(`Signaling server running on port ${PORT}`);

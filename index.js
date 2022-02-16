// Import the functions you need from the SDKs you need
import {initializeApp} from 'firebase/app';
import {
    addDoc,
    arrayRemove,
    arrayUnion,
    collection,
    deleteDoc,
    doc,
    getDoc,
    getDocs,
    getFirestore,
    limit,
    orderBy,
    query,
    setDoc,
    where
} from 'firebase/firestore';
import logger from 'morgan';
import http from 'http';
import bodyParser from 'body-parser';
import express from 'express';
import request from 'request';
import MessengerPlatform from 'facebook-bot-messenger';

// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional

const
    FIREBASE_apiKey = process.env.FIREBASE_apiKey,
    FIREBASE_authDomain = process.env.FIREBASE_authDomain,
    FIREBASE_projectId = process.env.FIREBASE_projectId,
    FIREBASE_storageBucket = process.env.FIREBASE_storageBucket,
    FIREBASE_messagingSenderId = process.env.FIREBASE_messagingSenderId,
    FIREBASE_appId = process.env.FIREBASE_appId,
    FIREBASE_measurementId = process.env.FIREBASE_measurementId,

    FB_pageId = process.env.FB_pageId,
    FB_appId = process.env.FB_appId,
    FB_appSecret = process.env.FB_appSecret,
    FB_validationToken = process.env.FB_validationToken,
    FB_pageToken = process.env.FB_pageToken;

const firebaseConfig = {
    apiKey: FIREBASE_apiKey,
    authDomain: FIREBASE_authDomain,
    projectId: FIREBASE_projectId,
    storageBucket: FIREBASE_storageBucket,
    messagingSenderId: FIREBASE_messagingSenderId,
    appId: FIREBASE_appId,
    measurementId: FIREBASE_measurementId,
};

// Initialize Firebase
// import serviceKey from 'lqdchatventure-firebase-adminsdk-p777u-b92ccc8457.json';

const initFirebase = initializeApp(firebaseConfig);

// Initialize Cloud Firestore through Firebase
const db = getFirestore();

var router = express();

var app = express();
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: false
}));

var server = http.createServer(app);

var bot = MessengerPlatform.create({
    pageID: FB_pageId,
    appID: FB_appId,
    appSecret: FB_appSecret,
    validationToken: FB_validationToken,
    pageToken: FB_pageToken
}, server);

app.use(bot.webhook('/webhook'));
bot.on(MessengerPlatform.Events.MESSAGE, function (userId, message) {
    // add code below.
});

app.listen(process.env.PORT || 3001);

var timeout = {};
var help_list = [
    'https://i.imgur.com/SfBllHn.png',
    'https://i.imgur.com/6xaJvKh.png',
    'https://i.imgur.com/Lk6Gy6c.png',
    'https://i.imgur.com/6loTnSI.png',
    'https://i.imgur.com/D67W2gm.png',
    'https://i.imgur.com/yTee0uQ.png',
    'https://i.imgur.com/AtwqtLi.png',
    'https://i.imgur.com/tzxBegc.png',
    'https://i.imgur.com/VSVc3t6.png',
    'https://i.imgur.com/4oAyWRR.png',
    'https://i.imgur.com/jQOZy50.png',
    'https://i.imgur.com/MulVA9m.png',
    'https://i.imgur.com/vC49OV4.png',
    'https://i.imgur.com/jXkyqHw.png',
    'https://i.imgur.com/2Lz39OL.png',
    'https://i.imgur.com/WVsKaAL.png',
]

app.get('/', (req, res) => {
    res.send('Heh');
});

//Validation token facebook webhook
app.get('/webhook', function (req, res) {
    if (req.query['hub.verify_token'] === FB_validationToken) {
        res.send(req.query['hub.challenge']);
    }
    res.send('Error, wrong validation token');
});

// Đoạn code xử lý khi có người nhắn tin cho bot
app.post('/webhook', function (req, res) {
        //sendReq(req);
        (async () => {
            try {
                var entries = req.body.entry;
                for (var entry of entries) {
                    var messaging = entry.messaging;
                    for (var message of messaging) {
                        // // console.log('messages');

                        let senderId = message.sender.id;

                        // // console.log('Async');
                        var senderData = await getDoc(doc(db, 'users', senderId));
                        var command_text = true;

                        if (message.message || message.postback || message.reaction) {

                            if (!senderData.exists()) {
                                if (senderId === '105254598184667') return;

                                // Check kết nối lần đầu, setup profile
                                await bot.sendTextMessage(senderId, 'Hey. Lần đầu? Bạn có thể sẽ muốn ' +
                                    'đọc qua hướng dẫn bằng cách gõ \'help\' trước đấy'
                                );
                                await bot.sendTextMessage(senderId, 'Đầu tiên bạn có thể sẽ muốn đặt nickname của' +
                                    ' bản thân bằng cú pháp : \n nickname < nickname của bạn > (Mặc định : \'Ẩn danh\')');

                                await bot.sendTextMessage(senderId, "Hoặc là thiết lập giới tính của mình bằng lệnh \n gioitinh + < nam | nu | khongdat >\n(Mặc định : khongdat)");

                                await sendQuickReply(senderId, "Thiết lập profile của bạn có thể giúp dễ dàng tìm kiếm hiệu quả và hợp lý hơn " +
                                    "cho bạn và mọi người");

                                await getProfile(senderId).then(async function (profile) {
                                    // // console.log('First connected');

                                    let docRef = await addDoc(collection(db, 'global_vars', 'masks', 'users'), {
                                        id: senderId
                                    });

                                    await setDoc(doc(db, 'users', senderId), {
                                        nickname: 'Ẩn danh',
                                        gender: profile.gender === undefined ? null : profile.gender,
                                        fb_link: null,
                                        history_requesting_timestamp: null,
                                        last_connect: null,
                                        id: senderId,
                                        topic: null,
                                        partner: null,
                                        crr_timestamp: null,
                                        age: null,
                                        age_range: null,
                                        tags: [],
                                        find_tags: [],
                                        find_gender: null,
                                        blocked: [],
                                        queued_timestamp: null,
                                        answered_questions: [],
                                        // asked_questions: [],
                                        listen_to_queue: true,
                                        exclude_last_connected: false,
                                        mask_id: docRef.id,
                                        qa_requesting_id: null,
                                        crr_question: null,
                                    });

                                    await setDoc(doc(db, 'names', profile.name + ' ' + profile.id), {
                                        id: senderId
                                    });

                                }).catch((err) => console.log(err));
                                senderData = await getDoc(doc(db, 'users', senderId));
                            }
                        }
                        if (message.message) {

                            // Nếu người dùng gửi tin nhắn đến
                            if (message.message.text) {

                                var text = message.message.text;
                                // // console.log(senderId, text);

                                await setDoc(doc(db, 'users', senderId), {
                                    last_text: text,
                                }, {merge: true});

                                if (['ketnoi', 'timkiem', 'kết nối', 'tìm kiếm', 'ket noi', 'tim kiem'].includes(text.toLowerCase())) {
                                    try {
                                        await addToQueue(senderId, senderData);
                                    } catch
                                        (e) {
                                        // Deal with the fact the chain failed
                                        // console.log('Error somewhere:', e);
                                    }

                                } else if (['profile', 'thông tin', 'hồ sơ', 'thong tin', 'ho so', 'thongtin', 'hoso'].includes(text.toLowerCase())) {

                                    let link = 'https://lqdchatventure-web.herokuapp.com/profile?id=' + senderData.data().mask_id;
                                    let elements = [{
                                        'title': 'Profile của bạn',
                                        'default_action': {
                                            'type': 'web_url',
                                            'url': link,
                                            'webview_height_ratio': 'full',
                                        },
                                        'buttons': [
                                            {
                                                'type': 'web_url',
                                                'url': link,
                                                'title': 'Profile của bạn'
                                            }
                                        ]
                                    }];
                                    await sendList(senderId, elements);

                                } else if (['thoát', 'thoat', 'kết thúc', 'ket thuc', 'ketthuc'].includes(text.toLowerCase())) {

                                    let code = await getOut(senderId, senderData);
                                    if (code === 0)
                                        await sendQuickReply(senderId, 'Không thể thoát khi chưa kết nối hoặc trong hàng đợi');

                                } else if (['timkiemnangcao', 'tìm kiếm nâng cao', 'tim kiem nang cao', 'advance search', 'advance_search', 'advancesearch']
                                    .includes(text.toLowerCase())) {
                                    let link = 'https://lqdchatventure-web.herokuapp.com/advance_search?id=' + senderData.data().mask_id;
                                    let elements = [{
                                        'title': 'Sửa đổi các thiết lập tìm kiếm nâng cao',
                                        'default_action': {
                                            'type': 'web_url',
                                            'url': link,
                                            'webview_height_ratio': 'full',
                                        },
                                        'buttons': [
                                            {
                                                'type': 'web_url',
                                                'url': link,
                                                'title': 'Tìm kiếm nâng cao'
                                            }
                                        ]
                                    }];
                                    await sendList(senderId, elements);
                                } else if (['lệnh', 'lenh'].includes(text.toLowerCase())) {

                                    //Query cho lịch sử
                                    let remove_elements = [];
                                    let command_elements = [];
                                    let queryHistory = query(collection(db, 'users', senderId, 'history')
                                        , orderBy('timestamp', 'desc')
                                        , limit(10)
                                    )

                                    let querySnapshot = await getDocs(queryHistory);

                                    let i = querySnapshot.size;

                                    // // console.log('Size : ', i);

                                    if (i === 0) await sendQuickReply(senderId, 'Bạn chưa tham gia cuộc trò chuyện nào');

                                    //Setup Buttons + Elements
                                    querySnapshot.forEach((gettedDoc) => {

                                        let data = gettedDoc.data();
                                        let command_buttons = [];
                                        let title;

                                        title = data.nickname === null
                                            ? 'Người lạ' : data.nickname;
                                        if (data.set_nickname !== null) title += ' ( ' + data.set_nickname + ' )';

                                        let remove_buttons = [
                                            {
                                                'type': 'postback',
                                                'title': 'Xóa cuộc trò chuyện',
                                                'payload': 'DELETE_HISTORY_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                            },
                                            {
                                                'type': 'postback',
                                                'title': 'Block',
                                                'payload': 'BLOCK_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                            }
                                        ]

                                        if (i === querySnapshot.size && senderData.data().crr_timestamp !== null) {
                                            command_buttons = [
                                                {
                                                    'type': 'postback',
                                                    'title': 'Gửi in4',
                                                    'payload': 'POST_INFO_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                                }
                                            ];
                                        } else {
                                            if (data.requesting) {
                                                command_buttons = [
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Đồng ý kết nối',
                                                        'payload': 'ACCEPT_REQUEST_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                                    },
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Từ chối yêu cầu',
                                                        'payload': 'REJECT_REQUEST_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                                    }
                                                ]
                                            } else if (data.requested) {
                                                command_buttons = [
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Hủy yêu cầu',
                                                        'payload': 'REMOVE_REQUEST_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                                    }
                                                ]
                                            } else {
                                                command_buttons = [
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Gửi lời mời kết nối',
                                                        'payload': 'CONNECT_REQUEST_PAYLOAD ' + gettedDoc.id + ' ' + data.psid
                                                    }
                                                ]
                                            }
                                        }

                                        if (data.fb_link !== null) {
                                            command_buttons.push(
                                                {
                                                    'type': 'web_url',
                                                    'title': 'Mở link Facebook',
                                                    'url': data.fb_link,
                                                }
                                            )
                                        }

                                        command_elements.push(
                                            {
                                                'title': title,
                                                'subtitle': timeConverter(data.timestamp),
                                                'default_action': {
                                                    'type': 'web_url',
                                                    'url': data.fb_link !== null ? data.fb_link
                                                        : 'https://www.facebook.com/lqdchatventure',
                                                    'webview_height_ratio': 'full',
                                                },
                                                'buttons': command_buttons
                                            }
                                        );
                                        i--;

                                        remove_elements.push(
                                            {
                                                'title': title,
                                                'subtitle': timeConverter(data.timestamp),
                                                'default_action': {
                                                    'type': 'web_url',
                                                    'url': data.fb_link !== null ? data.fb_link
                                                        : 'https://www.facebook.com/',
                                                    'webview_height_ratio': 'full',
                                                },
                                                'buttons': remove_buttons
                                            }
                                        );
                                    });
                                    await sendList(senderId, command_elements);
                                    await sendList(senderId, remove_elements);
                                } else if (['tìm nam', 'tim nam', 'timnam'].includes(text.toLowerCase())) {
                                    await setDoc(doc(db, 'users', senderId), {

                                        find_gender: 'male',

                                    }, {merge: true});
                                    await addToQueue(senderId, senderData);

                                } else if (['tìm nữ', 'tim nu', 'timnu'].includes(text.toLowerCase())) {
                                    await setDoc(doc(db, 'users', senderId), {

                                        find_gender: 'female',

                                    }, {merge: true});
                                    await addToQueue(senderId, senderData);

                                } else if (['trợ giúp', 'tro giup', 'trogiup', 'help'].includes(text.toLowerCase())) {
                                    for (let help in help_list) {
                                        await bot.sendImageMessage(senderId, help_list[help]);
                                    }
                                    await sendQuickReply(senderId, "Chào mừng đến với Chatventure");
                                } else if (['tìm câu hỏi', 'tim cau hoi', 'timcauhoi'].includes(text.toLowerCase())) {

                                    let docSnap = await getDoc(doc(db, 'global_vars', 'qa'));
                                    let questIdList = [];
                                    for (let i = 0; i < docSnap.data().questions_count; i++) {
                                        // console.log(i, questIdList)

                                        if (senderData.data().answered_questions.includes(i)) continue;

                                        questIdList.push(i);
                                    }

                                    if (questIdList.length === 0) {
                                        bot.sendTextMessage(senderId, "Hiện không còn câu hỏi nào mà bạn chưa trả lời. Hãy đến đây " +
                                            "vào lúc khác")
                                    } else {
                                        let randQuestion = Math.floor(Math.random() * questIdList.length);


                                        let querySnapshot = await getDocs(query(collection(db, 'questions'),
                                            where('random_id', '==', randQuestion)));

                                        querySnapshot.forEach((docSnap) => {
                                            (async () => {
                                                // doc.data() is never undefined for query doc snapshots
                                                // console.log(doc.id, " => ", doc.data());

                                                await sendQuickReply(senderId, 'Câu hỏi : ' + docSnap.data().text);

                                                await setDoc(doc(db, 'users', senderId), {

                                                    crr_question: docSnap.id,

                                                }, {merge: true});

                                            })();
                                        });

                                    }

                                } else if (['câu hỏi của tôi', 'cau hoi cua toi', 'cauhoicuatoi'].includes(text.toLowerCase())) {
                                    let link = 'https://lqdchatventure-web.herokuapp.com/quest?id=' + senderData.data().mask_id;
                                    let elements = [{
                                        'title': 'Các câu hỏi bạn đã đặt',
                                        'default_action': {
                                            'type': 'web_url',
                                            'url': link,
                                            'webview_height_ratio': 'full',
                                        },
                                        'buttons': [
                                            {
                                                'type': 'web_url',
                                                'url': link,
                                                'title': 'Câu hỏi của bạn'
                                            }
                                        ]
                                    }];
                                    await sendList(senderId, elements);
                                } else if (['câu hỏi đã trả lời', 'cau hoi đã trả lời', 'cauhoidatraloi', 'datraloi', 'đã trả lời', 'da tra loi'].includes(text.toLowerCase())) {
                                    let link = 'https://lqdchatventure-web.herokuapp.com/answered?id=' + senderData.data().mask_id;
                                    let elements = [{
                                        'title': 'Các câu hỏi bạn đã trả lời',
                                        'default_action': {
                                            'type': 'web_url',
                                            'url': link,
                                            'webview_height_ratio': 'full',
                                        },
                                        'buttons': [
                                            {
                                                'type': 'web_url',
                                                'url': link,
                                                'title': 'Câu hỏi bạn đã trả lời'
                                            }
                                        ]
                                    }];
                                    await sendList(senderId, elements);
                                } else if (['câu hỏi hiện tại', 'cau hoi hien tai', 'cauhoihientai'].includes(text.toLowerCase())) {
                                    if (senderData.data().crr_question === null || senderData.data().crr_question === undefined) {
                                        await sendQuickReplyQuestion(senderId, 'Bạn chưa tìm kiếm câu hỏi nào cả');
                                    } else {
                                        let questData = await getDoc(doc(db, 'questions', senderData.data().crr_question))
                                        bot.sendTextMessage(senderId, "Câu hỏi hiện tại : " + questData.data().text);
                                    }
                                } else if (['block'].includes(text.toLowerCase())) {
                                    await blockFunc(senderId, senderData, senderData.data().partner);
                                } else if (['báo bug', 'báo lỗi', 'baobug', 'baoloi'].includes(text.toLowerCase())) {
                                    await setDoc(doc(db, "global_vars", "bug"), {
                                        bugger: arrayUnion(senderId),
                                    }, {merge: true})
                                } else if (['resetacc', 'reset acc'].includes(text.toLowerCase())) {
                                    //Reset acc

                                    await getOut();

                                    let queryHistory = query(collection(db, 'users', senderId, 'history'));

                                    let querySnapshot = await getDocs(queryHistory);

                                    querySnapshot.forEach((gettedDoc) => {
                                        (async () => {
                                            await deleteDoc(doc(db, 'users', senderId, 'history', gettedDoc.id));
                                            await deleteDoc(doc(db, 'users', psid, 'history', gettedDoc.id));
                                        })();
                                    });

                                    await deleteDoc(doc(db, 'global_vars', 'masks', 'users', senderData.data().mask_id));

                                    let docRef = await addDoc(collection(db, 'global_vars', 'masks', 'users'), {
                                        id: senderId
                                    });

                                    await setDoc(doc(db, 'users', senderId), {
                                        nickname: 'Ẩn danh',
                                        gender: profile.gender === undefined ? null : profile.gender,
                                        fb_link: null,
                                        history_requesting_timestamp: null,
                                        last_connect: null,
                                        id: senderId,
                                        topic: null,
                                        partner: null,
                                        crr_timestamp: null,
                                        age: null,
                                        age_range: null,
                                        tags: [],
                                        find_tags: [],
                                        find_gender: null,
                                        blocked: [],
                                        queued_timestamp: null,
                                        answered_questions: [],
                                        // asked_questions: [],
                                        listen_to_queue: true,
                                        exclude_last_connected: false,
                                        mask_id: docRef.id,
                                        qa_requesting_id: null,
                                    });

                                    await sendQuickReply(senderId, 'Bạn đã reset acc thành công');

                                } else if (checkIfParameterCmd(text)) {

                                    var command = text.substr(0, text.indexOf(' '));
                                    var parameter = text.substr(text.indexOf(' ') + 1);

                                    // // console.log(command, parameter);

                                    if (command.toLowerCase() === '-admin-global' && (senderId === '4007404939324313' || senderId === '5654724101269283')) {
                                        let all = query(collection(db, 'users'));

                                        let querySnapshot = await getDocs(all);

                                        querySnapshot.forEach((gettedDoc) => {
                                            (async () => {
                                                try {
                                                    await bot.sendTextMessage(gettedDoc.id, parameter);
                                                } catch (e) {
                                                    console.log("Err: " + e);
                                                }
                                            })();
                                        });
                                        await bot.sendTextMessage(senderId, "Đã gửi tin nhắn global");
                                    } else if (command.toLowerCase() === 'fblink') {
                                        await setDoc(doc(db, 'users', senderId), {

                                            fb_link: parameter,

                                        }, {merge: true});
                                        await sendQuickReply(senderId, 'Link FB của bạn đã được cập nhật');
                                    } else if (command.toLowerCase() === 'nickname') {
                                        await setDoc(doc(db, 'users', senderId), {

                                            nickname: parameter,

                                        }, {merge: true});

                                        await sendQuickReply(senderId, 'Nickname của bạn đã được đặt là ' + parameter);

                                        if (senderData.data().crr_timestamp !== null) {
                                            await setDoc(doc(db, 'users', senderData.data().partner,
                                                'history', senderData.data().crr_timestamp.toString()), {

                                                nickname: parameter,

                                            }, {merge: true});
                                        }

                                    } else if (command.toLowerCase() === 'gioitinh') {

                                        if (parameter !== 'nam' && parameter !== 'nu' && parameter !== 'khongdat') {
                                            await sendQuickReplyGender(senderId, 'Giới tính không hợp lệ. Các giới tính có thể' +
                                                ' đặt : nam | nu | khongdat');
                                            return;
                                        }

                                        let set_gender;
                                        if (parameter === 'nam') set_gender = 'male';
                                        else if (parameter === 'nu') set_gender = 'female';
                                        else set_gender = null;

                                        await setDoc(doc(db, 'users', senderId), {

                                            gender: set_gender,

                                        }, {merge: true});

                                        await sendQuickReply(senderId, 'Giới tính của bạn đã được đặt là : ' + parameter);

                                    } else if (command.toLowerCase() === 'datnickname') {

                                        if (senderData.data().crr_timestamp === null) {
                                            await sendQuickReply(senderId, 'Bạn chưa kết nối với ai');
                                        } else {
                                            await setDoc(doc(db, 'users', senderId,
                                                'history', senderData.data().crr_timestamp.toString()), {

                                                set_nickname: parameter,

                                            }, {merge: true});

                                            await bot.sendTextMessage(senderId, 'Bạn đã đặt nickname cho đối tượng là ' + parameter);
                                        }
                                    } else if (command.toLowerCase() === 'cauhoi' || command.toLowerCase() === 'ask') {

                                        // // console.log('Help');

                                        let docSnap = await getDoc(doc(db, 'global_vars', 'qa'));

                                        // // console.log('Quests count : ', docSnap.data().questions_count)

                                        await addDoc(collection(db, 'questions'), {

                                            text: parameter,
                                            answers_count: 0,
                                            timestamp: Date.now(),
                                            author: senderData.data().nickname,
                                            author_id: senderId,
                                            random_id: docSnap.data().questions_count

                                        });

                                        // // console.log('Hye');

                                        await setDoc(doc(db, 'global_vars', 'qa'), {

                                            questions_count: docSnap.data().questions_count + 1,

                                        }, {merge: true});

                                        await sendQuickReplyQuestion(senderId, "Câu hỏi của bạn đã được ghi lại");

                                    } else if (command.toLowerCase() === '-admin-system-quest' && senderId === '4007404939324313') {

                                        // // console.log('Help');

                                        let docSnap = await getDoc(doc(db, 'global_vars', 'qa'));

                                        // // console.log('Quests count : ', docSnap.data().questions_count)

                                        await addDoc(collection(db, 'questions'), {

                                            text: parameter,
                                            answers_count: 0,
                                            timestamp: Date.now(),
                                            author: 'SYSTEM',
                                            author_id: 'SYSTEM',
                                            random_id: docSnap.data().questions_count

                                        });

                                        // // console.log('Hye');

                                        await setDoc(doc(db, 'global_vars', 'qa'), {

                                            questions_count: docSnap.data().questions_count + 1,

                                        }, {merge: true});

                                        await bot.sendTextMessage(senderId, "Câu hỏi hệ thống đã được ghi lại");

                                    } else if (command.toLowerCase() === 'traloi') {
                                        if (senderData.data().crr_question === null) {
                                            await sendQuickReplyQuestion(senderId, 'Bạn chưa tìm kiếm câu hỏi nào cả');
                                        } else {
                                            let docSnapQuestions = await getDoc(doc(db, 'questions', senderData.data().crr_question));

                                            await addDoc(collection(db, 'questions', senderData.data().crr_question, 'answers'), {
                                                text: parameter,
                                                timestamp: Date.now(),
                                                author: senderData.data().nickname,
                                                author_id: senderId,
                                            });

                                            await setDoc(doc(db, 'questions', senderData.data().crr_question), {
                                                answers_count: docSnapQuestions.data().answers_count + 1,
                                            }, {merge: true});

                                            await setDoc(doc(db, 'users', senderId), {
                                                answered_questions: arrayUnion(docSnapQuestions.data().random_id),
                                                crr_question: null,
                                            }, {merge: true});

                                            let link = 'https://lqdchatventure-web.herokuapp.com/ans?questId=' + docSnapQuestions.id +
                                                '&id=' + senderData.data().mask_id;

                                            let elements = [{
                                                'title': 'Câu hỏi của bạn đã được ghi lại',
                                                'default_action': {
                                                    'type': 'web_url',
                                                    'url': link,
                                                    'webview_height_ratio': 'full',
                                                },
                                                'buttons': [
                                                    {
                                                        'type': 'web_url',
                                                        'url': link,
                                                        'title': 'Các câu trả lời khác'
                                                    }
                                                ]
                                            }];
                                            await sendList(senderId, elements);
                                        }
                                    } else if (command.toLowerCase() === 'phongdoi') {

                                        if (parameter === 'khong') {
                                            await setDoc(doc(db, 'users', senderId), {
                                                listen_to_queue: false
                                            }, {merge: true});
                                            await sendQuickReplyQueue(senderId, 'Bạn đã thoát khỏi phòng đợi. ' +
                                                'Để tiếp tục tham gia phòng đợi hãy nhập \'phongdoi co\'', false
                                            )

                                        } else if (parameter === 'co') {
                                            await setDoc(doc(db, 'users', senderId), {
                                                listen_to_queue: true
                                            }, {merge: true});
                                            await sendQuickReplyQueue(senderId, 'Bạn đã tham gia phòng đợi. ' +
                                                'Để thoát khỏi phòng đợi hãy nhập \'phongdoi khong\'', true
                                            )
                                        }
                                    }
                                } else {
                                    //Tin nhắn thường

                                    if (senderData.data().crr_timestamp !== null) {
                                        command_text = false;
                                        await bot.sendTextMessage(senderData.data().partner, text);

                                    } else if (senderData.data().listen_to_queue === true &&
                                        (senderData.data().queued_timestamp !== null
                                            || senderData.data().history_requesting_timestamp !== null
                                            || senderData.data().qa_requesting_id !== null)) {

                                        command_text = false;

                                        await sendQueueTextMessage(senderId, text);

                                    } else {
                                        await sendQuickReply(senderId, 'Câu lệnh không hợp lệ. Bạn chưa kết nối với ai cả');
                                    }

                                }
                            } else {
                                //Attachments

                                if (senderData.data().crr_timestamp === null
                                    && senderData.data().history_requesting_timestamp === null
                                    && senderData.data().queued_timestamp === null
                                    && senderData.data().qa_requesting_id === null) {

                                    await sendQuickReply(senderId, 'Bạn đang không kết nối với ai cả');

                                } else {

                                    for (var attachments of message.message.attachments) {
                                        var type = attachments.type;
                                        var payload = attachments.payload.url;

                                        if (senderData.data().crr_timestamp !== null) {
                                            command_text = false;

                                            if (type === 'image') {
                                                await bot.sendImageMessage(senderData.data().partner, payload)
                                            } else if (type === 'audio') {
                                                await bot.sendAudioMessage(senderData.data().partner, payload)
                                            } else if (type === 'video') {
                                                await bot.sendVideoMessage(senderData.data().partner, payload)
                                            } else if (type === 'file') {
                                                await bot.sendFileMessage(senderData.data().partner, payload)
                                            }
                                        } else if (senderData.data().listen_to_queue === true &&
                                            (senderData.data().queued_timestamp !== null
                                                || senderData.data().history_requesting_timestamp !== null
                                                || senderData.data().qa_requesting_id !== null)) {

                                            command_text = false;

                                            let docRef = doc(db, 'global_vars', 'queue');
                                            let docSnap = await getDoc(docRef);

                                            for (let queued_user_index in docSnap.data().queue_list) {
                                                let queued_user = docSnap.data().queue_list[queued_user_index];

                                                let docRefQueueUser = doc(db, 'users', queued_user);
                                                let docSnapQueueUser = await getDoc(docRefQueueUser);

                                                if (docSnapQueueUser.data().listen_to_queue === true
                                                    && queued_user !== senderId) {

                                                    if (type === 'image') {
                                                        await bot.sendTextMessage(queued_user, senderData.data().nickname + " đã gửi 1 ảnh");
                                                        await bot.sendImageMessage(queued_user, payload);
                                                    } else if (type === 'audio') {
                                                        await bot.sendTextMessage(queued_user, senderData.data().nickname + " đã gửi 1 đoạn âm thanh");
                                                        await bot.sendAudioMessage(queued_user, payload);
                                                    } else if (type === 'video') {
                                                        await bot.sendTextMessage(queued_user, senderData.data().nickname + " đã gửi 1 video");
                                                        await bot.sendVideoMessage(queued_user, payload);
                                                    } else if (type === 'file') {
                                                        await bot.sendTextMessage(queued_user, senderData.data().nickname + " đã gửi 1 file");
                                                        await bot.sendFileMessage(queued_user, payload);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (command_text) await sendQuickReply(senderId);
                        } else if (message.postback) {

                            //Payload
                            let payload = message.postback.payload.split(' ');
                            var timestamp = parseInt(payload[1]);
                            var psid = payload[2];

                            //Payload yêu cầu kết nối
                            if (payload[0] === 'CONNECT_REQUEST_PAYLOAD') {
                                //Check nếu đang trong hàng đợi hoặc đã kết nối
                                if (senderData.data().crr_timestamp !== null
                                    || senderData.data().queued_timestamp !== null
                                    || senderData.data().history_requesting_timestamp !== null
                                    || senderData.data().qa_requesting_id !== null) {
                                    await bot.sendTextMessage(senderId, 'Bạn phải không đang yêu cầu hoặc kết nối với ai. Hãy nhập lệnh thoát trước khi ' +
                                        'gửi yêu cầu kết nối');
                                } else {

                                    //Check nếu người kia cũng đang yêu cầu connect
                                    let docRef = doc(db, 'users', psid, 'history', timestamp.toString());
                                    let docSnap = await getDoc(docRef);

                                    if (!docSnap.exists()) {
                                        bot.sendTextMessage(senderId, 'Lịch sử cuộc trò chuyện này đã bị xóa bởi bạn ' +
                                            'hoặc đối tác');
                                        return;
                                    }

                                    if (docSnap.data().requested === true) {
                                        await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {

                                            requesting: false,

                                        }, {merge: true});

                                        await setDoc(doc(db, 'users', psid, 'history', timestamp.toString()), {

                                            requested: false

                                        }, {merge: true});

                                        await connect(senderId, psid);
                                    } else {
                                        //Sửa request, vào hàng đợi
                                        await setDoc(doc(db, 'users', senderId), {

                                            history_requesting_timestamp: timestamp,

                                        }, {merge: true});

                                        await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {

                                            requested: true

                                        }, {merge: true});

                                        await setDoc(doc(db, 'global_vars', 'queue'), {

                                            queue_list: arrayUnion(senderId)

                                        }, {merge: true});

                                        await sendQueueTextMessage(senderId, senderData.data().nickname + ' đã tham gia phòng đợi');

                                        await bot.sendTextMessage(senderId, 'Đã gửi yêu cầu kết nối cho ' +
                                            'người dùng lúc ' + timeConverter(timestamp));

                                        await setDoc(doc(db, 'users', psid, 'history', timestamp.toString()), {

                                            requesting: true,

                                        }, {merge: true});

                                        await bot.sendTextMessage(psid, 'Người dùng lúc ' + timeConverter(timestamp)
                                            + ' đã gửi yêu cầu kết nối');

                                        let docRef = doc(db, 'users', psid, 'history', timestamp.toString());
                                        let docSnap = await getDoc(docRef);

                                        let title;

                                        title = docSnap.data().nickname === null
                                            ? 'Người lạ' : docSnap.data().nickname;
                                        if (docSnap.data().set_nickname !== null) title += ' ( ' + docSnap.data().set_nickname + ' )';

                                        let command_elements = [
                                            {
                                                'title': title,
                                                'subtitle': timeConverter(docSnap.data().timestamp),
                                                'default_action': {
                                                    'type': 'web_url',
                                                    'url': docSnap.data().fb_link !== null ? docSnap.data().fb_link
                                                        : 'https://www.facebook.com/',
                                                    'webview_height_ratio': 'full',
                                                },
                                                'buttons': [
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Đồng ý kết nối',
                                                        'payload': 'ACCEPT_REQUEST_PAYLOAD ' + timestamp + ' ' + senderId
                                                    },
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Từ chối yêu cầu',
                                                        'payload': 'REJECT_REQUEST_PAYLOAD ' + timestamp + ' ' + senderId
                                                    }
                                                ]
                                            }
                                        ];


                                        let remove_elements = [
                                            {
                                                'title': title,
                                                'subtitle': timeConverter(docSnap.data().timestamp),
                                                'default_action': {
                                                    'type': 'web_url',
                                                    'url': docSnap.data().fb_link !== null ? docSnap.data().fb_link
                                                        : 'https://www.facebook.com/',
                                                    'webview_height_ratio': 'full',
                                                },
                                                'buttons': [
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Xóa cuộc trò chuyện',
                                                        'payload': 'DELETE_HISTORY_PAYLOAD ' + timestamp + ' ' + senderId
                                                    },
                                                    {
                                                        'type': 'postback',
                                                        'title': 'Block',
                                                        'payload': 'BLOCK_PAYLOAD ' + timestamp + ' ' + senderId
                                                    }
                                                ]
                                            }
                                        ];

                                        await sendList(psid, command_elements);
                                        await sendList(psid, remove_elements);
                                    }
                                }
                            }
                            //Payload đồng ý request
                            else if (payload[0] === 'ACCEPT_REQUEST_PAYLOAD') {

                                //Check nếu trong hàng đợi hoặc đã kết nối
                                if (senderData.data().crr_timestamp !== null) {
                                    await bot.sendTextMessage(senderId, ' Bạn phải không trong hàng đợi / yêu cầu ' +
                                        'kết nối tới người khác. Hãy \'thoat\' trước khi đồng ý kết nối');
                                } else {
                                    //Check nếu yêu cầu còn hiệu lực

                                    let docRef = doc(db, 'users', psid, 'history', timestamp.toString());
                                    let docSnap = await getDoc(docRef);
                                    if (docSnap.data().requested === true) {

                                        await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {

                                            requesting: false,

                                        }, {merge: true});

                                        await setDoc(doc(db, 'users', psid, 'history', timestamp.toString()), {

                                            requested: false

                                        }, {merge: true});

                                        await connect(senderId, psid);
                                    } else {

                                        //Lời mời không còn hiệu lực khi timestamp của người mời đã hủy requested
                                        await bot.sendTextMessage(senderId, 'Người dùng hiện không còn yêu cầu kết nối tới bạn');
                                    }
                                }
                            }
                            //Payload xóa yêu cầu
                            else if (payload[0] === 'REMOVE_REQUEST_PAYLOAD') {

                                let docRef = doc(db, 'users', senderId, 'history', timestamp.toString());
                                let docSnap = await getDoc(docRef);
                                //Nếu yêu cầu đã bị xóa (Người kia từ chối hoặc mình exit hoặc đã remove)
                                if (!docSnap.exists() || docSnap.data().requested === false) {
                                    await bot.sendTextMessage(senderId, 'Bạn đã không còn yêu cầu kết nối với người dùng này ' +
                                        'hoặc người yêu cầu đã hủy lời mời');
                                } else {

                                    //Xóa queued và history_requesting_timestamp
                                    await setDoc(doc(db, 'users', senderId), {

                                        history_requesting_timestamp: null,

                                    }, {merge: true});

                                    //Xóa requested của timestamp
                                    await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {

                                        requested: false,

                                    }, {merge: true});

                                    //Xóa queue
                                    await setDoc(doc(db, 'global_vars', 'queue'), {

                                        queue_list: arrayRemove(senderId)

                                    }, {merge: true});

                                    await sendQueueTextMessage(senderId, senderData.data().nickname + ' đã thoát khỏi phòng đợi');

                                    //Xóa requesting bên kia timestamp
                                    await setDoc(doc(db, 'users', psid, 'history', timestamp.toString()), {

                                        requesting: false

                                    }, {merge: true});

                                    await bot.sendTextMessage(senderId, 'Bạn đã hủy yêu cầu kết nối');
                                }

                            }
                            //Payload từ chối yêu cầu
                            else if (payload[0] === 'REJECT_REQUEST_PAYLOAD') {

                                //Check yêu cầu còn hiệu lực
                                let docRef = doc(db, 'users', psid, 'history', timestamp.toString());
                                let docSnap = await getDoc(docRef);

                                //Nếu yêu cầu hết hiệu lực vì đã bị rejected, người kia remove hoặc exit
                                if (docSnap.data().requested === false) {
                                    await bot.sendTextMessage(senderId, 'Bạn đã không còn yêu cầu kết nối với người dùng này ' +
                                        'hoặc người yêu cầu đã hủy lời mời');
                                } else {

                                    //Remove requesting bên timestamp mình
                                    await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {

                                        requesting: false,

                                    }, {merge: true});

                                    //Remove requested bên timestamp họ
                                    await setDoc(doc(db, 'users', psid, 'history', timestamp.toString()), {

                                        requested: false

                                    }, {merge: true});

                                    //Remove history_requesting_timestamp của họ
                                    await setDoc(doc(db, 'users', psid), {

                                        history_requesting_timestamp: null,

                                    }, {merge: true});

                                    //Xóa queue
                                    await setDoc(doc(db, 'global_vars', 'queue'), {

                                        queue_list: arrayRemove(psid)

                                    }, {merge: true});

                                    let docSnapNickname = await getDoc(doc(db, 'users', psid));
                                    await sendQueueTextMessage(psid, docSnapNickname.data().nickname + ' đã thoát khỏi phòng đợi');

                                    await bot.sendTextMessage(senderId, 'Bạn đã từ chối cầu kết nối');
                                    await bot.sendTextMessage(psid, 'Người dùng vào lúc ' + timeConverter(timestamp)
                                        + ' đã từ chối yêu cầu kết nối của bạn');
                                }

                            }
                            //Payload yêu cầu trao đổi in4
                            else if (payload[0] === 'POST_INFO_PAYLOAD') {

                                if (senderData.data().crr_timestamp === timestamp) {

                                    if (senderData.data().fb_link === null) {
                                        await bot.sendTextMessage(senderId, 'Bạn chưa thiết lập tài khoản FB')
                                    } else {
                                        await setDoc(doc(db, 'users', psid,
                                            'history', senderData.data().crr_timestamp.toString()), {
                                            fb_link: senderData.data().fb_link
                                        }, {merge: true});

                                        await bot.sendTextMessage(senderId, 'Đã gửi');
                                        await bot.sendTextMessage(psid, 'Đã nhận ' + senderData.data().fb_link);
                                    }
                                } else {
                                    await bot.sendTextMessage(senderId, 'Bạn không còn kết nối với người này')
                                }
                            } else if (payload[0] === 'DELETE_HISTORY_PAYLOAD') {
                                let docRef = doc(db, 'users', psid, 'history', timestamp.toString());
                                let docSnap = await getDoc(docRef);

                                if (!docSnap.exists()) {
                                    await bot.sendTextMessage(psid, 'Lịch sử đã bị xóa từ trước')
                                } else {
                                    let queryHistory = query(collection(db, 'users', senderId, 'history')
                                        , where('psid', '==', psid)
                                    )

                                    let querySnapshot = await getDocs(queryHistory);

                                    querySnapshot.forEach((gettedDoc) => {
                                        (async () => {
                                            await deleteDoc(doc(db, 'users', senderId, 'history', gettedDoc.id));
                                            await deleteDoc(doc(db, 'users', psid, 'history', gettedDoc.id));
                                        })();
                                    });

                                    //Nếu đang kết nối
                                    if (senderData.data().partner === psid) {
                                        //Hủy kết nối cho partner
                                        await setDoc(doc(db, 'users', psid), {
                                            partner: null,
                                            // nickname: null,
                                            queued_timestamp: null,
                                            history_requesting_timestamp: null,
                                            crr_timestamp: null
                                            // gender: profile.gender === undefined ? null : profile.gender,
                                            // token: Math.floor(Math.random() * 69420),
                                            // age: null,
                                        }, {merge: true});

                                        //Reset
                                        await setDoc(doc(db, 'users', senderId), {
                                            partner: null,
                                            // nickname: null,
                                            queued_timestamp: null,
                                            history_requesting_timestamp: null,
                                            crr_timestamp: null
                                            // gender: profile.gender === undefined ? null : profile.gender,
                                            // token: Math.floor(Math.random() * 69420),
                                            // age: null,
                                        }, {merge: true});

                                        await bot.sendTextMessage(psid, 'Bạn đã bị xóa lịch sử với người này thành công');
                                    }
                                    await bot.sendTextMessage(senderId, 'Bạn đã xóa lịch sửs với người này thành công');
                                }
                            } else if (payload[0] === 'BLOCK_PAYLOAD') {
                                await blockFunc(senderId, senderData, psid);
                            } else {
                                let psid = payload[1];

                                if (payload[0] === 'QA_ACCEPT_REQUEST_PAYLOAD') {

                                    //Check nếu trong hàng đợi hoặc đã kết nối
                                    if (senderData.data().crr_timestamp !== null) {
                                        await bot.sendTextMessage(senderId, ' Bạn phải không trong hàng đợi / yêu cầu ' +
                                            'kết nối tới người khác. Hãy \'thoat\' trước khi đồng ý kết nối');
                                    } else {
                                        //Check nếu yêu cầu còn hiệu lực

                                        let docRef = doc(db, 'users', psid);
                                        let docSnap = await getDoc(docRef);
                                        if (docSnap.data().qa_requesting_id === senderId) {
                                            await connect(senderId, psid);
                                        } else {
                                            //Lời mời không còn hiệu lực khi timestamp của người mời đã hủy requested
                                            await bot.sendTextMessage(senderId, 'Người dùng hiện không còn yêu cầu kết nối tới bạn');
                                        }
                                    }
                                } else if (payload[0] === 'QA_REJECT_REQUEST_PAYLOAD') {
                                    let docRef = doc(db, 'users', psid);
                                    let docSnap = await getDoc(docRef);
                                    if (docSnap.data().qa_requesting_id === senderId) {
                                        await setDoc(doc(db, 'users', psid), {
                                            qa_requesting_id: null,
                                        }, {merge: true});
                                        await bot.sendTextMessage(psid, 'Rejected');
                                        await bot.sendTextMessage(senderId, 'Yêu cầu kết nối của bạn đã bị từ chối');

                                        await setDoc(doc(db, 'global_vars', 'queue'), {
                                            queue_list: arrayRemove(psid)
                                        }, {merge: true});

                                    } else {
                                        //Lời mời không còn hiệu lực khi timestamp của người mời đã hủy requested
                                        await bot.sendTextMessage(senderId, 'Người dùng hiện không còn yêu cầu kết nối tới bạn');
                                    }
                                } else if (payload[0] === 'QA_REMOVE_REQUEST_PAYLOAD') {

                                    if (senderData.data().qa_requesting_id === psid) {
                                        await setDoc(doc(db, 'users', senderId), {
                                            qa_requesting_id: null,
                                        }, {merge: true})

                                        await setDoc(doc(db, 'global_vars', 'queue'), {
                                            queue_list: arrayRemove(senderId)
                                        }, {merge: true});

                                    } else {
                                        //Lời mời không còn hiệu lực khi timestamp của người mời đã hủy requested
                                        await bot.sendTextMessage(senderId, 'Bạn không còn yêu cầu kết nối tới người dùng');
                                    }
                                }
                            }
                        }
                        //Reaction
                        else if (message.reaction) {

                            let react = message.reaction;
                            if (react.action === 'react')
                                await bot.sendTextMessage(senderData.data().partner, 'Đã thả react ' + react.emoji);
                            else
                                await bot.sendTextMessage(senderData.data().partner, 'Đã xóa react');

                            // if (senderData.data().crr_timestamp !== null) {
                            //     let react = message.reaction;
                            //     getTextbyMID(react.mid).then(function (message) {
                            //             var textMID = message
                            //
                            //             var reactMessage
                            //             if (react.action === 'react')
                            //                 reactMessage = 'Đã thả ' + react.emoji + ' tin nhắn :\n\n' +
                            //                     textMID;
                            //             else
                            //                 reactMessage = 'Đã xóa react tin nhắn : \n\n' +
                            //                     textMID;
                            //
                            //             // console.log(senderData.data().partner, reactMessage);
                            //
                            //             (async () => {
                            //                 await bot.sendTextMessage(senderData.data().partner, reactMessage);
                            //             })();
                            //
                            //         }
                            //     ).catch((err) => // console.log(err));
                            // }
                        }
                    }
                }
            } catch (e) {
                console.log("BUG: ", e);
            }
        })();
        res.status(200).send('OK');
    }
)

// Request tới pipedream debug
function sendReq(rq) {
    request({
        url: 'https://en8p31fkk54ombz.m.pipedream.net/',
        method: 'POST',
        json: rq
    });
}

function getTextbyMID(mid) {
    // console.log(mid);
    return new Promise(function (resolve, reject) {
        request({
            url: 'https://graph.facebook.com/v12.0/' + mid + '?fields=message',
            qs: {
                access_token: FB_pageToken,
            },
            method: 'GET',
        }, function (err, res, body) {
            if (err) {
                // console.log(err);
                reject(err);
                return;
            }
            try {
                // JSON.parse() can throw an exception if not valid JSON
                resolve(body.message);
                // // console.log(body);
                // resolve(body);
            } catch (e) {
                reject(e);
            }
        });
    })
}

function getProfile(id) {
    return new Promise(function (resolve, reject) {
        request({
            url: 'https://graph.facebook.com/v12.0/' + id + '?fields=gender',
            qs: {
                access_token: FB_pageToken,
            },
            method: 'POST',
        }, function (err, res, body) {
            if (err) {
                // console.log(err);
                reject(err);
                return;
            }
            try {
                // JSON.parse() can throw an exception if not valid JSON
                resolve(JSON.parse(body));
                // // console.log(JSON.parse(body));
                // resolve(body);
            } catch (e) {
                reject(e);
            }
        });
    })
}

async function sendList(senderID, elements) {
    await request({
        url: 'https://graph.facebook.com/v12.0/me/messages',
        qs: {
            access_token: FB_pageToken,
        },
        method: 'POST',
        json: {
            'recipient': {
                'id': senderID
            },
            'message': {
                'attachment': {
                    'type': 'template',
                    'payload': {
                        'template_type': 'generic',
                        'elements': elements
                    }
                }
            }
        }
    });
}

function timeConverter(timestamp) {
    var a = new Date(timestamp);
    // var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    // var year = a.getFullYear();
    // var month = months[a.getMonth()];
    // var date = a.getDate();
    // var hour = a.getHours();
    // var min = a.getMinutes();
    // var sec = a.getSeconds();
    // return date + ' ' + month + ' ' + year + ' ' + hour + ':' + min + ':' + sec;
    return a.toLocaleString('vi-VN', {timeZone: 'Asia/Ho_Chi_Minh'});
}

async function connect(senderId, gettedId) {
    let timestamp = Date.now();

    await setDoc(doc(db, 'users', senderId), {
        partner: gettedId,
        history_requesting_timestamp: null,
        crr_timestamp: timestamp,
        last_connect: gettedId,
        queued_timestamp: null,
        find_gender: null,
        qa_requesting_id: null,
    }, {merge: true});

    let docRef = doc(db, 'users', gettedId);
    let docSnap = await getDoc(docRef);

    await setDoc(doc(db, 'users', senderId, 'history', timestamp.toString()), {
        timestamp: timestamp,
        psid: gettedId,
        tags: null,
        nickname: docSnap.data().nickname,
        set_nickname: null,
        fb_link: null,
        img: null,
        requested: false,
        requesting: false,
    }, {merge: true});

    await setDoc(doc(db, 'users', gettedId), {
        partner: senderId,
        history_requesting_timestamp: null,
        crr_timestamp: timestamp,
        last_connect: senderId,
        queued_timestamp: null,
        find_gender: null,
        qa_requesting_id: null,

    }, {merge: true});

    docRef = doc(db, 'users', senderId);
    docSnap = await getDoc(docRef);

    await setDoc(doc(db, 'users', gettedId, 'history', timestamp.toString()), {
        timestamp: timestamp,
        psid: senderId,
        tags: null,
        nickname: docSnap.data().nickname,
        set_nickname: null,
        fb_link: null,
        img: null,
        requested: false,
        requesting: false,
    }, {merge: true});

    //Xóa queue
    await setDoc(doc(db, 'global_vars', 'queue'), {
        queue_list: arrayRemove(gettedId)
    }, {merge: true});

    await setDoc(doc(db, 'global_vars', 'queue'), {
        queue_list: arrayRemove(senderId)
    }, {merge: true});

    let docSnapNickname = await getDoc(doc(db, 'users', gettedId));

    await sendQueueTextMessage(gettedId, docSnapNickname.data().nickname + ' đã được kết nối và thoát khỏi phòng đợi');
    await bot.sendTextMessage(senderId, 'Bạn đã được kết nối. Nói lời chào với người bạn mới đi nào');
    await bot.sendTextMessage(gettedId, 'Bạn đã được kết nối. Nói lời chào với người bạn mới đi nào');

}

async function addToQueue(senderId, senderData) {
    senderData = senderData.data();

    // Check nếu trong hàng đợi hoặc đã kết nối
    if (senderData.queued_timestamp !== null || senderData.crr_timestamp !== null
        || senderData.history_requesting_timestamp !== null || senderData.qa_requesting_id !== null) {
        await bot.sendTextMessage(senderId, 'Bạn phải không kết nối hoặc đang yêu cầu / trong hàng đợi với ai');
        return;
    }

    //Query cho người đang trong queued
    var queryForQueued = query(collection(db, 'users')
        , where('queued_timestamp', '!=', null)
        , orderBy('queued_timestamp')
    )

    let querySnapshot = await getDocs(queryForQueued);
    let find = null;
    //Nếu tồn tại thì kết nối

    for (var x in querySnapshot.docs) {
        let gettedDoc = querySnapshot.docs[x];
        let gettedDocData = gettedDoc.data();

        // // console.log('hey ' + gettedDocData.last_connect);

        if (gettedDocData.exclude_last_connected === true && gettedDocData.last_connect === senderId) continue;
        if (senderData.exclude_last_connected === true && senderData.last_connect === gettedDoc.id) continue;
        // console.log('Not last connected');

        // // console.log('ayo ', gettedDocData.find_tags,
        //     gettedDocData.id, senderData.find_tags);

        if (senderData.find_tags.length !== 0) {
            // // console.log('true1', senderData.find_tags.length);
            if (!senderData.find_tags.some(
                tags => gettedDocData.tags.includes(tags))) continue;
        }

        // // console.log('aloha ' + senderData.find_tags);

        if (gettedDocData.find_tags.length !== 0) {
            //// console.log('true2', gettedDocData.find_tags.length);
            if (!gettedDocData.find_tags.some(
                tags => senderData.tags.includes(tags))) continue;
        }

        // console.log('Tags match');

        // console.log(gettedDocData.age_range, senderData.age_range);

        if (gettedDocData.age_range !== null) {
            if (senderData.age === null) continue;
            if (gettedDocData.age_range[0] > senderData.age
                || senderData.age > gettedDocData.age_range[1])
                continue;
        }

        if (senderData.age_range !== null) {
            if (gettedDocData.age === null) continue;
            if (senderData.age_range[0] > gettedDocData.age
                || gettedDocData.age > senderData.age_range[1])
                continue;

        }

        // console.log('Age range match');

        if (senderData.find_gender !== null && senderData.find_gender !== gettedDocData.gender) continue;
        if (gettedDocData.find_gender !== null && gettedDocData.find_gender !== senderData.gender) continue;

        // console.log('Gender match');

        if (senderData.blocked.includes(gettedDoc.id)) continue;
        if (gettedDocData.blocked.includes(senderId)) continue;

        // console.log('Not blocked');

        find = gettedDoc.id;
        //// console.log('aloho', find);

        break;
    }

    //Nếu queued không thỏa mãn
    if (find === null) {
        await setDoc(doc(db, 'users', senderId), {
            queued_timestamp: Date.now()
        }, {merge: true});

        await setDoc(doc(db, 'global_vars', 'queue'), {
            queue_list: arrayUnion(senderId)
        }, {merge: true});

        let docSnapNickname = await getDoc(doc(db, 'users', senderId));
        await sendQueueTextMessage(senderId, docSnapNickname.data().nickname + ' đã tham gia phòng đợi');

        await bot.sendTextMessage(senderId, 'Đang chờ kết nối. Bạn đã được đưa vào hàng chờ');

    } else {
        await connect(senderId, find);
    }
}

function checkIfParameterCmd(text) {
    let command = text.substr(0, text.indexOf(' '));
    if (command.toLowerCase() === '') {
        // bot.sendTextMessage(senderId, "Lệnh không hợp lệ");
        return false;

    }
    return ['-admin-global', 'fblink', 'nickname', 'datnickname', 'cauhoi', 'traloi', 'phongdoi', 'ask', 'gioitinh', "-admin-system-quest"]
        .includes(command.toLowerCase());
}

async function sendQueueTextMessage(senderId, text) {
    let docRef = doc(db, 'global_vars', 'queue');
    let docSnap = await getDoc(docRef);

    docRef = doc(db, 'users', senderId);
    let senderData = await getDoc(docRef);
    let nickname = senderData.data().nickname;

    for (let queued_user_index in docSnap.data().queue_list) {
        let queued_user = docSnap.data().queue_list[queued_user_index];
        let docRefQueueUser = doc(db, 'users', queued_user);
        let docSnapQueueUser = await getDoc(docRefQueueUser);
        // // console.log(docSnap.data().queue_list, queued_user, docSnapQueueUser.data());
        if (docSnapQueueUser.data().listen_to_queue === true
            && queued_user !== senderId) {
            try {
                await bot.sendTextMessage(queued_user, nickname + " :\n\n" + text);
            } catch (e) {
                console.log("Error at sendQueueTextMessage:", e);
            }
        }

    }
}

async function sendQuickReply(senderId, text) {
    try {
        await request({
            url: 'https://graph.facebook.com/v12.0/me/messages',
            qs: {
                access_token: FB_pageToken,
            },
            method: 'POST',
            json: {
                "recipient": {
                    "id": senderId,
                },
                "messaging_type": "RESPONSE",
                "message": {
                    "text": text,
                    "quick_replies": [
                        {
                            "content_type": "text",
                            "title": "Tìm kiếm",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://icons-for-free.com/iconfiles/png/512/search-131964753234672616.png"
                        },
                        {
                            "content_type": "text",
                            "title": "Tìm nam",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://icons.iconarchive.com/icons/custom-icon-design/flatastic-7/512/Male-icon.png"
                        }, {
                            "content_type": "text",
                            "title": "Tìm nữ",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://icons.iconarchive.com/icons/custom-icon-design/flatastic-7/512/Female-icon.png"
                        }, {
                            "content_type": "text",
                            "title": "Lệnh",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://image.flaticon.com/icons/png/512/59/59130.png"
                        }, {
                            "content_type": "text",
                            "title": "Hồ sơ",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://cdn.iconscout.com/icon/free/png-256/profile-417-1163876.png"
                        }, {
                            "content_type": "text",
                            "title": "Tìm kiếm nâng cao",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://static.thenounproject.com/png/2161054-200.png"
                        }, {
                            "content_type": "text",
                            "title": "Tìm câu hỏi",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://icon-library.com/images/question-icon/question-icon-0.jpg"
                        }, {
                            "content_type": "text",
                            "title": "Câu hỏi của tôi",
                            "payload": "RANDOM_PAYLOAD",
                            "image_url": "https://i.imgur.com/YwjvVyv.png"
                        },
                    ]
                }
            }
        });
    } catch (e) {
        console.log("Error at sendQuickReply : ", e);
    }
}

async function sendQuickReplyGender(senderId, text) {
    await request({
        url: 'https://graph.facebook.com/v12.0/me/messages',
        qs: {
            access_token: FB_pageToken,
        },
        method: 'POST',
        json: {
            "recipient": {
                "id": senderId,
            },
            "messaging_type": "RESPONSE",
            "message": {
                "text": text,
                "quick_replies": [
                    {
                        "content_type": "text",
                        "title": "gioitinh nam",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://icons.iconarchive.com/icons/custom-icon-design/flatastic-7/512/Male-icon.png"
                    },
                    {
                        "content_type": "text",
                        "title": "gioitinh nu",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://icons.iconarchive.com/icons/custom-icon-design/flatastic-7/512/Female-icon.png"
                    }, {
                        "content_type": "text",
                        "title": "gioitinh khongdat",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://i.imgur.com/BYunYX8.png"
                    }
                ]
            }
        }
    });
}


async function sendQuickReplyQuestion(senderId, text) {
    await request({
        url: 'https://graph.facebook.com/v12.0/me/messages',
        qs: {
            access_token: FB_pageToken,
        },
        method: 'POST',
        json: {
            "recipient": {
                "id": senderId,
            },
            "messaging_type": "RESPONSE",
            "message": {
                "text": text,
                "quick_replies": [
                    {
                        "content_type": "text",
                        "title": "Tìm câu hỏi",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://icon-library.com/images/question-icon/question-icon-0.jpg"
                    }, {
                        "content_type": "text",
                        "title": "Câu hỏi hiện tại",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://i.pinimg.com/736x/15/8b/ed/158bed9819e4fccf7e18a5eeeaf79c6b.jpg"
                    }, {
                        "content_type": "text",
                        "title": "Câu hỏi của tôi",
                        "payload": "RANDOM_PAYLOAD",
                        "image_url": "https://i.pinimg.com/736x/15/8b/ed/158bed9819e4fccf7e18a5eeeaf79c6b.jpg"
                    },
                ]
            }
        }
    });
}

async function sendQuickReplyQueue(senderId, text, queue) {
    let option;
    if (queue) option = 'phongdoi khong'
    else option = 'phongdoi co';
    await request({
        url: 'https://graph.facebook.com/v12.0/me/messages',
        qs: {
            access_token: FB_pageToken,
        },
        method: 'POST',
        json: {
            "recipient": {
                "id": senderId,
            },
            "messaging_type": "RESPONSE",
            "message": {
                "text": text,
                "quick_replies": [
                    {
                        "content_type": "text",
                        "title": option,
                        "payload": "RANDOM_PAYLOAD",
                    },
                ]
            }
        }
    });
}

async function blockFunc(senderId, senderData, psid) {
    if (psid === null) {
        await sendQuickReply(senderId, 'Bạn đang không kết nối với ai cả để block');
        return;
    }

    let blocked = senderData.data().blocked;
    if (blocked.includes(psid)) {
        await sendQuickReply(senderId, 'Bạn đã block người này sẵn từ trước');
    } else {
        blocked.push(psid);
        await setDoc(doc(db, 'users', senderId), {
            blocked: blocked
        }, {merge: true});

        let queryHistory = query(collection(db, 'users', senderId, 'history')
            , where('psid', '==', psid)
        )

        let querySnapshot = await getDocs(queryHistory);

        querySnapshot.forEach((gettedDoc) => {
            (async () => {
                await deleteDoc(doc(db, 'users', senderId, 'history', gettedDoc.id));
                await deleteDoc(doc(db, 'users', psid, 'history', gettedDoc.id));
            })();
        });

        //Nếu đang kết nối
        if (senderData.data().partner === psid) {
            //Hủy kết nối cho partner
            await setDoc(doc(db, 'users', psid), {
                partner: null,
                // nickname: null,
                crr_timestamp: null
            }, {merge: true});

            //Reset
            await setDoc(doc(db, 'users', senderId), {
                partner: null,
                // nickname: null,
                crr_timestamp: null
            }, {merge: true});

            await sendQuickReply(psid, 'Bạn đã bị block thành công');
        }
        try {
            await sendQuickReply(senderId, 'Bạn đã block thành công');
        } catch (e) {
            console.log("Error at blockFunc: ", e);
        }

    }
}

async function getOut(senderId, senderData) {
    try {

        //Check nếu đang trong hàng đợi hoặc đã kết nối hoặc đang request
        if (senderData.data().queued_timestamp === null
            && senderData.data().crr_timestamp === null
            && senderData.data().history_requesting_timestamp === null
            && senderData.data().qa_requesting_id === null) {
            return 0;
        }

        //Nếu đang kết nối
        if (senderData.data().crr_timestamp !== null) {
            await sendQuickReply(senderId, 'Bạn đã thoát khỏi cuộc trò chuyện với đối tác');

            //Hủy kết nối cho partner
            await setDoc(doc(db, 'users', senderData.data().partner), {
                partner: null,
                crr_timestamp: null,
                find_gender: null,
            }, {merge: true});

            await sendQuickReply(senderData.data().partner, 'Người kia đã thoát khỏi cuộc trò chuyện');
        } else if (senderData.data().listen_to_queue) {

            let nickname = senderData.data().nickname;
            await sendQueueTextMessage(senderId, nickname + ' đã thoát khỏi hàng đợi');
            await sendQuickReply(senderId, 'Bạn đã thoát khỏi cuộc trò chuyện trong hàng đợi');

        }

        //Nếu đang request
        if (senderData.data().history_requesting_timestamp !== null) {

            //Hủy lời mời kết nối cho bản thân
            await setDoc(doc(db, 'users', senderId,
                'history', senderData.data().history_requesting_timestamp.toString()), {
                requested: false
            }, {merge: true});

            //Query lấy psid người được request
            let docRef = doc(db, 'users', senderId
                , 'history', senderData.data().history_requesting_timestamp.toString());
            let docSnapHistory = await getDoc(docRef);

            //Hủy lời mời cho người được request
            await setDoc(doc(db, 'users', docSnapHistory.data().psid
                , 'history', docSnapHistory.id), {
                requesting: false
            }, {merge: true});
        }

        //Reset
        await setDoc(doc(db, 'users', senderId), {
            partner: null,
            // nickname: null,
            history_requesting_timestamp: null,
            crr_timestamp: null,
            find_gender: null,
            queued_timestamp: null,
            qa_requesting_id: null,
        }, {merge: true});

        //Xóa queue
        await setDoc(doc(db, 'global_vars', 'queue'), {
            queue_list: arrayRemove(senderId)
        }, {merge: true});

        return 1;
    } catch (e) {
        // Deal with the fact the chain failed
        console.log('Exit error:', e);
        return -1;
    }
}
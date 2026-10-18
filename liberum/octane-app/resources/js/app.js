import Echo from 'laravel-echo';
import Pusher from 'pusher-js';
import { sendWhisper } from './whisper';

window.Pusher = Pusher;

const root = document.querySelector('#echo-demo');

if (root) {
    const events = document.querySelector('#events');
    const status = document.querySelector('#status');
    const statusDot = document.querySelector('#status-dot');
    const notificationPreview = document.querySelector('#notification-preview');
    const csrf = document.querySelector('meta[name="csrf-token"]').content;

    const append = (kind, message) => {
        const item = document.createElement('li');
        const time = new Date().toLocaleTimeString();
        item.innerHTML = `<time>${time}</time><strong>${kind}</strong><span></span>`;
        item.querySelector('span').textContent = message;
        events.prepend(item);
    };

    const echo = new Echo({
        broadcaster: 'pusher',
        key: root.dataset.key,
        wsHost: window.location.hostname,
        wsPort: Number(root.dataset.port),
        wssPort: Number(root.dataset.port),
        forceTLS: window.location.protocol === 'https:',
        disableStats: true,
        enabledTransports: ['ws', 'wss'],
        authEndpoint: '/broadcasting/auth',
    });

    echo.connector.pusher.connection.bind('state_change', ({ current }) => {
        status.textContent = current;
        statusDot.dataset.state = current;
        append('socket', current);
    });

    const channel = echo.private('demo')
        .subscribed(() => append('channel', 'private-demo subscribed'))
        .listen('.DemoMessage', payload => append('broadcast', `${payload.message} · ${payload.sentAt}`))
        .notification(payload => {
            notificationPreview.textContent = payload.message;
            append('notification', `${payload.message} · ${payload.sentAt}`);
        })
        .listenForWhisper('typing', payload => append('whisper', payload.message));

    document.querySelector('#broadcast-form').addEventListener('submit', async event => {
        event.preventDefault();
        const button = event.currentTarget.querySelector('button');
        button.disabled = true;

        try {
            const response = await fetch('/broadcast/message', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json',
                    'X-CSRF-TOKEN': csrf,
                },
                body: JSON.stringify({ message: document.querySelector('#message').value }),
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            append('native', 'broadcast published');
        } catch (error) {
            append('error', error.message);
        } finally {
            button.disabled = false;
        }
    });

    document.querySelector('#notification').addEventListener('click', async event => {
        const button = event.currentTarget;
        button.disabled = true;

        try {
            const response = await fetch('/broadcast/notification', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                    'X-CSRF-TOKEN': csrf,
                },
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            append('native', 'notification published');
        } catch (error) {
            append('error', error.message);
        } finally {
            button.disabled = false;
        }
    });

    let whisperTimer;
    document.querySelector('#typing').addEventListener('input', event => {
        clearTimeout(whisperTimer);
        whisperTimer = setTimeout(() => {
            const message = event.target.value || 'stopped typing';
            const queued = sendWhisper(
                channel.subscription,
                echo.connector.pusher.connection.state,
                message,
            );
            append(queued ? 'whisper queued' : 'whisper not sent',
                queued ? message : 'Connection and channel subscription required');
        }, 120);
    });

    document.querySelector('#clear').addEventListener('click', () => events.replaceChildren());
}

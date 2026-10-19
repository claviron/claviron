import assert from 'node:assert/strict';
import test from 'node:test';
import { sendWhisper } from '../resources/js/whisper.js';

test('whisper requires a connected socket and a subscribed channel', () => {
    for (const [connectionState, subscribed] of [
        ['connecting', false], ['unavailable', true], ['connected', false],
    ]) {
        assert.equal(sendWhisper({ subscribed, trigger() {
            assert.fail('Must not send before connection and subscription');
        } }, connectionState, 'hello'), false);
    }
});

test('whisper reports the transport result, not peer delivery', () => {
    for (const accepted of [false, true]) {
        const calls = [];
        const subscription = { subscribed: true, trigger(...args) {
            calls.push(args);
            return accepted;
        } };
        assert.equal(sendWhisper(subscription, 'connected', 'hello'), accepted);
        assert.deepEqual(calls, [['client-typing', { message: 'hello' }]]);
    }
});

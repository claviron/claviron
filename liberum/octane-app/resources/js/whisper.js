export function sendWhisper(subscription, connectionState, message) {
    if (connectionState !== 'connected' || !subscription.subscribed) {
        return false;
    }

    return subscription.trigger('client-typing', { message });
}

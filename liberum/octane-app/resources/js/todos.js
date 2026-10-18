const root = document.querySelector('#todo-app');

if (root) {
    const endpoint = root.dataset.endpoint;
    const list = document.querySelector('#todos');
    const empty = document.querySelector('#empty');
    const errorBox = document.querySelector('#error');
    const search = document.querySelector('#search');

    let status = 'all';
    let term = '';
    let dragging = null;
    let orderBeforeDrag = [];

    const show = (error) => {
        errorBox.hidden = ! error;
        errorBox.textContent = error ?? '';
    };

    const request = async (path, options = {}) => {
        const response = await fetch(`${endpoint}${path}`, {
            headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
            ...options,
        });

        const payload = response.status === 204 ? {} : await response.json();

        if (! response.ok) {
            throw new Error(payload.message ?? `HTTP ${response.status}`);
        }

        return payload;
    };

    const applyCounts = (meta) => {
        if (! meta) {
            return;
        }

        for (const key of ['total', 'pending', 'completed']) {
            const node = document.querySelector(`[data-count="${key}"]`);
            if (node) {
                node.textContent = meta[key];
            }
        }
    };

    const dueLabel = (value) => {
        if (! value) {
            return null;
        }

        const due = new Date(`${value}T00:00:00`);
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const days = Math.round((due - today) / 86400000);

        if (days < 0) return { text: `${Math.abs(days)}d overdue`, state: 'overdue' };
        if (days === 0) return { text: 'today', state: 'today' };
        if (days === 1) return { text: 'tomorrow', state: 'soon' };

        return { text: `in ${days}d`, state: 'later' };
    };

    const row = (todo) => {
        const item = document.createElement('li');
        item.className = 'todo';
        item.dataset.id = todo.id;
        item.draggable = true;
        item.toggleAttribute('data-done', todo.completed);

        const check = document.createElement('button');
        check.type = 'button';
        check.className = 'check';
        check.setAttribute('aria-label', todo.completed ? 'Mark as open' : 'Mark as done');
        check.addEventListener('click', () => toggle(todo.id));

        const title = document.createElement('span');
        title.className = 'todo-title';
        title.textContent = todo.title;
        title.contentEditable = 'plaintext-only';
        title.spellcheck = false;
        title.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                event.preventDefault();
                title.blur();
            }
            if (event.key === 'Escape') {
                title.textContent = todo.title;
                title.blur();
            }
        });
        title.addEventListener('blur', () => {
            const next = title.textContent.trim();

            if (next === todo.title) {
                return;
            }

            if (next === '') {
                title.textContent = todo.title;

                return;
            }

            rename(todo.id, next);
        });

        const meta = document.createElement('span');
        meta.className = 'todo-meta';

        const priority = document.createElement('span');
        priority.className = 'badge';
        priority.dataset.priority = todo.priority;
        priority.textContent = todo.priority;
        meta.append(priority);

        const due = dueLabel(todo.due_on);
        if (due) {
            const badge = document.createElement('span');
            badge.className = 'badge due';
            badge.dataset.due = due.state;
            badge.textContent = due.text;
            meta.append(badge);
        }

        if (todo.notes) {
            const notes = document.createElement('span');
            notes.className = 'todo-notes';
            notes.textContent = todo.notes;
            meta.append(notes);
        }

        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'quiet remove';
        remove.textContent = '✕';
        remove.setAttribute('aria-label', 'Delete');
        remove.addEventListener('click', () => destroy(todo.id));

        item.append(check, title, meta, remove);

        return item;
    };

    const render = (todos) => {
        list.replaceChildren(...todos.map(row));
        empty.hidden = todos.length > 0;
    };

    const load = async () => {
        show(null);

        const query = new URLSearchParams();
        if (status !== 'all') query.set('status', status);
        if (term !== '') query.set('q', term);

        try {
            const payload = await request(query.size ? `?${query}` : '');
            render(payload.data);
            applyCounts(payload.meta);
        } catch (error) {
            show(error.message);
        }
    };

    const mutate = async (path, options) => {
        show(null);

        try {
            const payload = await request(path, options);
            applyCounts(payload.meta);
            await load();
        } catch (error) {
            show(error.message);
        }
    };

    const toggle = (id) => mutate(`/${id}/toggle`, { method: 'POST' });
    const destroy = (id) => mutate(`/${id}`, { method: 'DELETE' });
    const rename = (id, title) => mutate(`/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ title }),
    });

    document.querySelector('#composer').addEventListener('submit', async (event) => {
        event.preventDefault();

        const form = event.currentTarget;
        const button = form.querySelector('button');
        const title = document.querySelector('#title');

        button.disabled = true;

        await mutate('', {
            method: 'POST',
            body: JSON.stringify({
                title: title.value.trim(),
                priority: document.querySelector('#priority').value,
                due_on: document.querySelector('#due_on').value || null,
            }),
        });

        title.value = '';
        document.querySelector('#due_on').value = '';
        button.disabled = false;
        title.focus();
    });

    for (const tab of document.querySelectorAll('.tab')) {
        tab.addEventListener('click', () => {
            for (const other of document.querySelectorAll('.tab')) {
                other.classList.toggle('is-active', other === tab);
            }

            status = tab.dataset.status;
            load();
        });
    }

    let searchTimer;
    search.addEventListener('input', (event) => {
        clearTimeout(searchTimer);
        searchTimer = setTimeout(() => {
            term = event.target.value.trim();
            load();
        }, 180);
    });

    document.querySelector('#clear-completed').addEventListener('click', () => {
        mutate('/completed', { method: 'DELETE' });
    });

    // Reorder: the whole id list is posted, and the server writes it in one
    // transaction, so the page never shows a half-applied order.
    const currentOrder = () => [...list.querySelectorAll('.todo')].map((node) => Number(node.dataset.id));

    list.addEventListener('dragstart', (event) => {
        dragging = event.target.closest('.todo');
        dragging?.classList.add('is-dragging');
        orderBeforeDrag = currentOrder();
    });

    list.addEventListener('dragover', (event) => {
        event.preventDefault();

        const over = event.target.closest('.todo');

        if (! dragging || ! over || over === dragging) {
            return;
        }

        const { top, height } = over.getBoundingClientRect();
        const after = event.clientY > top + height / 2;

        over.parentNode.insertBefore(dragging, after ? over.nextSibling : over);
    });

    list.addEventListener('dragend', () => {
        dragging?.classList.remove('is-dragging');
        dragging = null;

        const ids = currentOrder();

        // A drag that ended where it started is not a reorder.
        if (ids.join() === orderBeforeDrag.join()) {
            return;
        }

        mutate('/reorder', { method: 'POST', body: JSON.stringify({ ids }) });
    });

    load();
}

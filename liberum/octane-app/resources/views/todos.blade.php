<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Claviron Todos</title>
    @vite(['resources/css/app.css', 'resources/js/todos.js'])
</head>
<body>
<main id="todo-app" data-endpoint="{{ url('/api/todos') }}">
    <header>
        <p class="eyebrow">CLAVIRON · LARAVEL · POSTGRESQL</p>
        <h1>Todos on the runtime pool.</h1>
        <p class="lead">
            Every read and write on this page goes through Eloquent to the native Claviron connection.
            The pool lives in Rust; the request never opens a connection of its own.
        </p>
        <div class="counters">
            <span class="counter"><strong data-count="pending">–</strong> open</span>
            <span class="counter"><strong data-count="completed">–</strong> done</span>
            <span class="counter"><strong data-count="total">–</strong> total</span>
        </div>
    </header>

    <section class="card composer">
        <h2>Add a todo</h2>
        <form id="composer">
            <input id="title" name="title" placeholder="What needs doing?" autocomplete="off" required maxlength="255">
            <select id="priority" name="priority" aria-label="Priority">
                <option value="low">low</option>
                <option value="normal" selected>normal</option>
                <option value="high">high</option>
            </select>
            <input id="due_on" name="due_on" type="date" aria-label="Due date">
            <button type="submit">Add</button>
        </form>
        <p class="hint">Validated by a form request, persisted through the pooled connection, returned as an API resource.</p>
    </section>

    <section class="card list-card">
        <div class="toolbar">
            <div class="tabs" role="tablist">
                <button type="button" class="tab is-active" data-status="all" role="tab">All</button>
                <button type="button" class="tab" data-status="pending" role="tab">Open</button>
                <button type="button" class="tab" data-status="completed" role="tab">Done</button>
            </div>
            <input id="search" type="search" placeholder="Search title and notes…" autocomplete="off">
            <button type="button" class="quiet" id="clear-completed">Clear done</button>
        </div>

        <ul id="todos" class="todos"></ul>
        <p id="empty" class="hint empty" hidden>Nothing here yet.</p>
        <p id="error" class="error" hidden></p>
    </section>

    <p class="hint footnote">Drag a row to reorder — the new order is written in one transaction on one pooled connection.</p>
</main>
</body>
</html>

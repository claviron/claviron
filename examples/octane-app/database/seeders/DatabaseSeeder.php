<?php

namespace Database\Seeders;

use App\Models\User;
use App\Models\Todo;
use App\Models\DemoRecord;
use Illuminate\Database\Console\Seeds\WithoutModelEvents;
use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    use WithoutModelEvents;

    /**
     * Seed the application's database.
     */
    public function run(): void
    {
        User::query()->updateOrCreate([
            'email' => 'demo@example.test',
        ], [
            'name' => 'Demo User',
            'email_verified_at' => now(),
            // Public demonstration credentials; never use outside this example.
            'password' => 'password',
        ]);

        foreach (['Open the Octane example', 'Read the same SQLite file from Orbit', 'Try the shared demo event'] as $position => $title) {
            Todo::query()->firstOrCreate(['title' => $title], ['position' => $position]);
        }

        DemoRecord::query()->firstOrCreate([
            'public_id' => '00000000-0000-4000-8000-000000000001',
        ], [
            'name' => 'Shared SQLite demo',
            'notes' => 'Synthetic data shared by the Examples applications.',
            'meta' => ['demo' => true],
            'settings' => ['source' => 'examples'],
        ]);
    }
}

<?php

require 'vendor/autoload.php';

use Illuminate\Support\Facades\Hash;

// Get the password from the command line arguments
if ($argc > 1) {
    $password = $argv[1];
    // Hash the password using Laravel's Hash::make
    $hashedPassword = Hash::make($password);
    echo $hashedPassword;
} else {
    echo "Please provide a password to hash.";
}
?>
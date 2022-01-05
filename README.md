#Setup
1. Install project dependencies by running command.
    ```
    npm run install
    ```
2. Run an instance of `Mariadb` and create database and tables by running the following command. Provide database username according to your settings.
    ```
    mysql -u root -p < Schema.sql
    ```

3. Create a `.env` file and provide following environment variables. Provide RPC_URL, database HOST, USER and PASSWORD according to your settings.
    ```
    RPC_URL=YOUR_RPC_API
    MARIADB_HOST=localhost
    MARIADB_USER=root
    MARIADB_PASSWORD=root
    MARIADB_DATABASE=Transfers
    MARIADB_CONNECTION_LIMIT=5 
   ```
4. Run the script using `node alchemy-script.js` and provide the list of addresses to scan the history for command lines arguments separated by a space. Like so.
    ```
    node alchemy-script.js 0x5e624faedc7aa381b574c3c2ff1731677dd2ee1d 0xaf648ffbc940570f3f6a9ca49b07ba7bc520bcdf
    ```
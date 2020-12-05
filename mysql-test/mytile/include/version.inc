

let $major_version = `SELECT CAST(SUBSTRING_INDEX(@@global.version, '.', 1) AS INT) AS major`;
let $minor_version = `SELECT CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(@@global.version, '.', 2), '.', -1) AS INT) AS minor`;

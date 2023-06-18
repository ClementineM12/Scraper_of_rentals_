print('Start #################################################################');

db = db.getSiblingDB('test');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'test' }],
  },
);
db.createCollection('rentals');
db.createCollection('most_recent');
db.createCollection('hrefs');

print('END #################################################################');

import { MongoClient } from 'mongodb';

const url = process.env.MONGO_URL || 'mongodb://localhost:27017/?replicaSet=rs0';
const g = globalThis;

export async function getDb() {
  if (!g._rutasurClient) {
    g._rutasurClient = new MongoClient(url).connect();
  }
  return (await g._rutasurClient).db('rutasur');
}

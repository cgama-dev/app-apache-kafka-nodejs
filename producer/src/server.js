const { Kafka } = require("kafkajs")

const config = {
    clientId: "my-app",
    brokers: ["localhost:9092"]
}

const kafka = new Kafka(config)

const producer = kafka.producer();

const randomNumber = () => Math.trunc(Math.random() * 999)

const sendMensage = () => {
    setInterval(async () => {
        let random = randomNumber()
        const messages = [
            { value: `${random}` }
        ]
        await producer.send({
            topic: 'topic-popcorn',
            messages,
        }, { codec: Kafka.COMPRESSION_GZIP })
    }, 1000)
}

async function start() {
    await producer.connect()
    sendMensage()
    // await producer.disconnect()
}

console.log(start())


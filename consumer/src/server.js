const { Kafka } = require("kafkajs")

const config = {
    clientId: "my-app",
    brokers: ["localhost:9092"]
}

const kafka = new Kafka(config)
const consumer = kafka.consumer({ groupId: 'test-group' })

async function main() {
    await consumer.connect()
    await consumer.subscribe({ topic: 'topic-popcorn', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            })
        },
    })
}

main()
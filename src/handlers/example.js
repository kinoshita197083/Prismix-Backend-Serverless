module.exports.hello = async (event) => {
    console.log(`ENV Test Variable: ${process.env.TEST}`);
    const env = process.env.TEST;

    return {
        statusCode: 200,
        body: JSON.stringify({ message: `Hello from ${env} serverless! testing...` }),
    };
};

/**
 * Prisma Example
 */

// const { PrismaClient } = require('@prisma/client')

// Initialize Prisma Client
// let prisma

// async function initPrismaClient() {
//   if (!prisma) {
//     prisma = new PrismaClient()
//     await prisma.$connect()
//   }
//   return prisma
// }

// const prisma = await new PrismaClient().$connect();

// module.exports.hello = async (event) => {
//   try {
//     const users = await prisma.user.findMany()
//     return {
//       statusCode: 200,
//       body: JSON.stringify({ message: 'Hello from Lambda!', users }),
//     }
//   } catch (error) {
//     console.error('Error:', error)
//     return {
//       statusCode: 500,
//       body: JSON.stringify({ message: 'Internal server error' }),
//     }
//   } finally {
//     await prisma.$disconnect()
//   }
// }
const { BlobServiceClient } = require('@azure/storage-blob');
const fs = require('fs');
const path = require('path');

// Configura tu cadena de conexión y nombre de contenedor
const connectStr = "DefaultEndpointsProtocol=https;AccountName=expdigitalmutuusprodv2;AccountKey=smgmE2QG7nsDdB1b38fYQOLoElEpFPJb1w9qVMDa7Li5s/R9J6Z+UJeXmcb38GzBYe7jL46pOZI/Eb2txCCO2w==;EndpointSuffix=core.windows.net";
const downloadFolder = "/Volumes/Data/Documentos";

// Asegúrate de que el directorio de descarga existe
if (!fs.existsSync(downloadFolder)) {
  fs.mkdirSync(downloadFolder);
}
// Función para descargar todos los blobs de un contenedor
async function downloadBlobsFromContainer(containerClient, containerName) {
  console.log(`Descargando blobs del contenedor: ${containerName}`);
  
  const blobs = containerClient.listBlobsFlat();
  
  for await (const blob of blobs) {
    try {
      const blobClient = containerClient.getBlobClient(blob.name);
      const downloadFilePath = path.join(downloadFolder, containerName, blob.name);
      
      // Asegúrate de que los directorios existen antes de descargar el archivo
      const dir = path.dirname(downloadFilePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      console.log(`Descargando archivo: ${blob.name}`);
      const downloadBlockBlobResponse = await blobClient.download(0);
      
      // Escribe el contenido en el archivo local
      const writableStream = fs.createWriteStream(downloadFilePath);
      downloadBlockBlobResponse.readableStreamBody.pipe(writableStream);
      
    } catch (err) {
      console.error(`Error al descargar el blob ${blob.name}:`, err.message);
    }
  }
}

// Función para listar y descargar todos los contenedores
async function downloadAllContainers() {
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectStr);
  const containers = blobServiceClient.listContainers();

  for await (const container of containers) {
    const containerClient = blobServiceClient.getContainerClient(container.name);
    
    // Descargar blobs dentro de cada contenedor
    await downloadBlobsFromContainer(containerClient, container.name);
  }

  console.log("Descarga completada.");
}

// Ejecutar la función principal
downloadAllContainers().catch((err) => {
  console.error("Error al descargar los blobs:", err.message);
});

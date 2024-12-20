const XLSX = require('xlsx');

const combineExcelFiles = () => {
  try {
    // Leer los archivos Excel
    const file1 = XLSX.readFile('/Users/moisem/Documents/Test/DocsMasiva/docs/Clientes.xlsx'); // Reemplaza con el nombre del primer archivo
    const file2 = XLSX.readFile('/Users/moisem/Documents/Test/DocsMasiva/docs/PlanPorClientePaginado.xlsx'); // Reemplaza con el nombre del segundo archivo

    // Convertir las hojas de trabajo a JSON
    const sheet1 = XLSX.utils.sheet_to_json(file1.Sheets[file1.SheetNames[0]]);
    const sheet2 = XLSX.utils.sheet_to_json(file2.Sheets[file2.SheetNames[0]]);
console.log(sheet1);
console.log(sheet2);

    const membershipMap = {};
    sheet2.forEach((row) => {
      const ids = row.ids.split(','); // Manejar IDs múltiples separados por comas
      ids.forEach((id) => {
        membershipMap[id.trim()] = row.mem;
      });
    });

    // Combinar los datos del archivo 1 con las membresías del archivo 2
    const combinedData = sheet1.map((row) => {
      const membership = membershipMap[row.ID] || 'Sin membresía';
      return {
        ...row,
        Membresía: membership,
      };
    });

    // Crear una nueva hoja de trabajo con los datos combinados
    const newWorkbook = XLSX.utils.book_new();
    const newWorksheet = XLSX.utils.json_to_sheet(combinedData);

    // Agregar la hoja al libro
    XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, 'Datos Combinados');

    // Guardar el nuevo archivo
    XLSX.writeFile(newWorkbook, 'Datos_Combinados.xlsx');
    console.log('Archivo combinado generado correctamente: Datos_Combinados.xlsx');
  } catch (error) {
    console.error('Error combinando los archivos Excel:', error.message);
  }
};

// Ejecutar la función
combineExcelFiles();

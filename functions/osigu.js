const { Client } = require("pg");
const ExcelJS = require("exceljs");
const path = require("path");
const fs = require("fs");
const stringify = require("csv-stringify").stringify;

async function filtrarPolizas(inputFilePath) {
  try {
    // Leer el archivo Excel
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(inputFilePath);

    // Obtener la primera hoja
    const worksheet = workbook.worksheets[0];

    // Leer los datos de las pólizas
    const polizasSet = new Set();
    worksheet.eachRow((row) => {
      const poliza = row.getCell(2).value; // Asume que las pólizas están en la columna 2
      if (typeof poliza === "number") {
        polizasSet.add(poliza);
      }
    });

    // Obtener la fecha de expiración (un año después de hoy)
    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 2);
    const formattedExpirationDate = expirationDate.toISOString();

    const options = {
      header: true,
      quoted: true, // Encloses all fields in double quotes
      escape: "\\", // Escapes double quotes inside fields
    };
    // Crear el primer archivo: v2_polices.csv
    const headers1 = [
      "INSURER_SLUG",
      "NUMBER",
      "ISSUED_DATE",
      "EFFECTIVE_DATE",
      "EXPIRATION_DATE",
      "INSURANCE_TYPE",
      "POLICY_TYPE",
      "COMPANY_NAME",
      "COMPANY_ADDRESS",
      "COMPANY_EMAIL_ADDRESS",
      "COMPANY_TAX_IDENTIFICATION_NUMBER",
      "CURRENCY_CODE",
      "NET_PREMIUM_AMOUNT",
      "STATUS",
    ];

    const rows1 = Array.from(polizasSet).map((poliza) => {
      const formattedPoliza = `M${poliza.toString().padStart(7, "0")}`;
      return {
        INSURER_SLUG: "mx-mutuus",
        NUMBER: formattedPoliza,
        ISSUED_DATE: "2022-01-01T00:00:00+00:00",
        EFFECTIVE_DATE: "2022-01-01T00:00:00+00:00",
        EXPIRATION_DATE: formattedExpirationDate,
        INSURANCE_TYPE: "HEALTH_INSURANCE",
        POLICY_TYPE: "GROUP",
        COMPANY_NAME:"MUTUUS",
        COMPANY_ADDRESS: "",
        COMPANY_EMAIL_ADDRESS: "",
        COMPANY_TAX_IDENTIFICATION_NUMBER: "1d13744156a548e9b85b5d41260e9153",
        CURRENCY_CODE: "MXN",
        NET_PREMIUM_AMOUNT: "",
        STATUS: "ENABLED",
      };
    });

    const csv1 = await new Promise((resolve, reject) => {
      stringify(
        rows1,
        { header: true, columns: headers1, quoted: true, escape: "\\" },
        (err, output) => {
          if (err) reject(err);
          else resolve(output);
        }
      );
    });
    const filePath1 = path.join(path.dirname(inputFilePath), "v2_policies.csv");
    fs.writeFileSync(filePath1, csv1, "utf8");
    console.log(`Archivo generado: ${filePath1}`);
    // Crear el segundo archivo: v2_groups.csv
    const headers2 = [
      "INSURER_SLUG",
      "POLICY_NUMBER",
      "CODE",
      "NAME",
      "CURRENCY_CODE",
      "MAX_LIFETIME_BENEFIT",
      "MAX_ANNUAL_BENEFIT",
      "DEDUCTIBLE_AMOUNT",
      "REIMBURSEMENT_BASE_AMOUNT",
      "STATUS",
    ];

    const rows2 = Array.from(polizasSet).map((poliza) => {
      const formattedPoliza = `M${poliza.toString().padStart(7, "0")}`;
      return {
        INSURER_SLUG: "mx-mutuus",
        POLICY_NUMBER: formattedPoliza,
        CODE: `${formattedPoliza}-1`,
        NAME: "Default group",
        CURRENCY_CODE: "MXN",
        MAX_LIFETIME_BENEFIT: 0,
        MAX_ANNUAL_BENEFIT: 5000000,
        DEDUCTIBLE_AMOUNT: 0,
        REIMBURSEMENT_BASE_AMOUNT: 0,
        STATUS: "ENABLED",
      };
    });

    const csv2 = await new Promise((resolve, reject) => {
      stringify(
        rows2,
        { header: true, columns: headers2, quoted: true, escape: "\\" },
        (err, output) => {
          if (err) reject(err);
          else resolve(output);
        }
      );
    });
    const filePath2 = path.join(path.dirname(inputFilePath), "v2_groups.csv");
    fs.writeFileSync(filePath2, csv2, "utf8");
    console.log(`Archivo generado: ${filePath2}`);

    // Crear el tercer archivo: v2_gr_coverages.csv
    const headers3 = [
      "INSURER_SLUG",
      "POLICY_NUMBER",
      "GROUP_CODE",
      "COVERAGE_TYPE",
      "DIRECT_BILLING_ENABLED",
    ];

    const rows3 = Array.from(polizasSet).map((poliza) => {
      const formattedPoliza = `M${poliza.toString().padStart(7, "0")}`;
      return {
        INSURER_SLUG: "mx-mutuus",
        POLICY_NUMBER: formattedPoliza,
        GROUP_CODE: `${formattedPoliza}-1`,
        COVERAGE_TYPE: "DIAGNOSTIC_TESTS",
        DIRECT_BILLING_ENABLED: "FALSE",
      };
    });

    const csv3 = await new Promise((resolve, reject) => {
      stringify(
        rows3,
        { header: true, columns: headers3, quoted: true, escape: "\\" },
        (err, output) => {
          if (err) reject(err);
          else resolve(output);
        }
      );
    });
    const filePath3 = path.join(path.dirname(inputFilePath), "v2_gr_coverages.csv");
    fs.writeFileSync(filePath3, csv3, "utf8");
    console.log(`Archivo generado: ${filePath3}`);

    const client = new Client({
      host: "dbmutuusv2prod.postgres.database.azure.com",
      database: "Mutuus",
      user: "mutuusreportes",
      password: "Mu7uus%Pr0%",
      port: 5432,
      ssl: { rejectUnauthorized: false },
    });

    await client.connect();

    const query = `
    WITH MembresiaConAsociado AS (
    SELECT 
        m."ID"::TEXT AS "MembresiaID_Modificado",
        m."Saldo",
        0 AS "OrdenTipo"
    FROM 
        public."Membresias" m
    INNER JOIN 
        public."Asociados" a ON m."AsociadoID" = a."ID"
    WHERE 
        m."Estatus" = 1 AND m."ProductoID" <> 5 -- Excluir el producto con ID 5
),
BeneficiariosConMembresia AS (
    SELECT 
        m."ID"::TEXT || '-' || ROW_NUMBER() OVER (PARTITION BY m."ID" ORDER BY b."ID") AS "MembresiaID_Modificado",
        b."Saldo", 
        1 AS "OrdenTipo"
    FROM 
        public."Membresias" m
    INNER JOIN 
        public."Asociados" a ON m."AsociadoID" = a."ID"
    INNER JOIN 
        public."Beneficiarios" b ON a."ID" = b."AsociadoID"
    WHERE 
        m."Estatus" = 1 AND m."ProductoID" <> 5 -- Excluir el producto con ID 5
)
SELECT 
    "MembresiaID_Modificado",
    "Saldo"
FROM (
    SELECT 
        "MembresiaID_Modificado",
        "Saldo",
        "OrdenTipo"
    FROM 
        MembresiaConAsociado
    UNION ALL
    SELECT 
        "MembresiaID_Modificado",
        "Saldo",
        "OrdenTipo"
    FROM 
        BeneficiariosConMembresia
) AS Resultado
ORDER BY 
    "OrdenTipo",
    (string_to_array("MembresiaID_Modificado", '-')::int[])[1], 
    (CASE 
        WHEN "MembresiaID_Modificado" LIKE '%-%' THEN (string_to_array("MembresiaID_Modificado", '-')::int[])[2] 
        ELSE NULL 
    END);
  `;

    const result = await client.query(query);

    const headers = [
      "INSURER_SLUG",
      "INSURED_ID",
      "DISEASE_CODE",
      "CURRENCY_CODE",
      "MAXIMUM_BENEFIT_AVAILABLE",
      "OUT_OF_POCKET_ACCRUED_AMOUNT",
      "DEDUCTIBLE_ACCRUED_AMOUNT",
    ];

    const rows = result.rows.map((row) => {
      return {
        INSURER_SLUG: "mx-mutuus",
        INSURED_ID: row.MembresiaID_Modificado,
        DISEASE_CODE: "",
        CURRENCY_CODE: "MXN",
        MAXIMUM_BENEFIT_AVAILABLE: row.Saldo,
        OUT_OF_POCKET_ACCRUED_AMOUNT: 0,
        DEDUCTIBLE_ACCRUED_AMOUNT: 0,
      };
    });
//v2_policyholder_balances.csv
   
    const csv = await new Promise((resolve, reject) => {
      stringify(
        rows,
        { header: true, columns: headers, quoted: true, escape: "\\" },
        (err, output) => {
          if (err) reject(err);
          else resolve(output);
        }
      );
    });
    const v2_policyholder_balances = path.join(path.dirname(inputFilePath), "v2_policyholder_balances.csv");
    fs.writeFileSync(v2_policyholder_balances, csv, "utf8");
    console.log(`Archivo generado: ${v2_policyholder_balances}`);
    await client.end();
    return {
      filePath1,
      filePath2,
      filePath3,
      v2_policyholder_balances,
    };
  } catch (error) {
    console.error("Error al procesar los archivos:", error);
    throw error;
  }
}

// Uso de la función
const inputFilePath = "docs/certpoli.xlsx"; // Cambia esto por la ruta a tu archivo

filtrarPolizas(inputFilePath)
  .then(
    async ({ filePath1, filePath2, filePath3, v2_policyholder_balances }) => {
      await generateCsvFile(inputFilePath),
        console.log(
          `Archivos creados:\n${filePath1}\n${filePath2}\n${filePath3}\n${v2_policyholder_balances}`
        );
    }
  )
  .catch((error) => console.error("Error:", error));

const generateExcelFile = async (inputFilePath) => {
  const client = new Client({
    host: "dbmutuusv2prod.postgres.database.azure.com",
    database: "Mutuus",
    user: "mutuusreportes",
    password: "Mu7uus%Pr0%",
    port: 5432,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  const query = `
    WITH MembresiaConAsociado AS (
        SELECT 
            m."ID"::TEXT AS "MembresiaID_Modificado",
            m."Saldo",
            0 AS "OrdenTipo",
            CONCAT(a."ApellidoParterno", ' ', a."ApellidoMaterno", ' ', a."Nombre") AS "NombreCompleto",
            a."Rfc" AS "Identificador",
            a."GeneroClave",
            a."FechaNacimiento",
            a."Email",
            a."Celular" AS "NumeroTelefono",
            m."FechaDesde",
            p."MontoProducto" AS "MontoProducto",
            CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
        FROM 
            public."Membresias" m
        INNER JOIN 
            public."Asociados" a ON m."AsociadoID" = a."ID"
        INNER JOIN 
            public."Productos" p ON m."ProductoID" = p."ID"
        LEFT JOIN 
            public."Domicilio" d ON a."DomicilioID" = d."ID"
        LEFT JOIN 
            public."Colonias" c ON d."ColoniaID" = c."ID"
        LEFT JOIN 
            public."Estados" e ON c."MunicipioID" = e."ID"
        WHERE 
            m."Estatus" = 1
    ),
    BeneficiariosConMembresia AS (
        SELECT 
            m."ID"::TEXT || '-' || ROW_NUMBER() OVER (PARTITION BY m."ID" ORDER BY b."ID") AS "MembresiaID_Modificado",
            b."Saldo", 
            1 AS "OrdenTipo",
            CONCAT(b."ApellidoPaterno", ' ', b."ApellidoMaterno", ' ', b."Nombre") AS "NombreCompleto",
            b."Curp" AS "Identificador",
            b."GeneroClave",
            b."FechaNacimiento",
            a."Email",
            a."Celular" AS "NumeroTelefono",
            m."FechaDesde",
            p."MontoProducto" AS "MontoProducto",
            CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
        FROM 
            public."Membresias" m
        INNER JOIN 
            public."Asociados" a ON m."AsociadoID" = a."ID"
        INNER JOIN 
            public."Beneficiarios" b ON a."ID" = b."AsociadoID"
        INNER JOIN 
            public."Productos" p ON m."ProductoID" = p."ID"
        LEFT JOIN 
            public."Domicilio" d ON a."DomicilioID" = d."ID"
        LEFT JOIN 
            public."Colonias" c ON d."ColoniaID" = c."ID"
        LEFT JOIN 
            public."Estados" e ON c."MunicipioID" = e."ID"
        WHERE 
            m."Estatus" = 1
    )
    SELECT 
        *
    FROM (
        SELECT * FROM MembresiaConAsociado
        UNION ALL
        SELECT * FROM BeneficiariosConMembresia
    ) AS Resultado
    ORDER BY 
        "OrdenTipo",
        (string_to_array("MembresiaID_Modificado", '-')::int[])[1], 
        (CASE 
            WHEN "MembresiaID_Modificado" LIKE '%-%' THEN (string_to_array("MembresiaID_Modificado", '-')::int[])[2] 
            ELSE NULL 
        END);
    `;

  const result = await client.query(query);

  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Membresias2");

  // Definir columnas
  worksheet.columns = [
    { header: "INSURER_SLUG", key: "INSURER_SLUG" },
    { header: "POLICY_NUMBER", key: "POLICY_NUMBER" },
    { header: "GROUP_CODE", key: "GROUP_CODE" },
    { header: "POLICYHOLDER_CERTIFICATE", key: "POLICYHOLDER_CERTIFICATE" },
    { header: "INSURED_ID", key: "INSURED_ID" },
    { header: "STATUS", key: "STATUS" },
    { header: "MAXIMUM_LIFETIME_BENEFIT", key: "MAXIMUM_LIFETIME_BENEFIT" },
    { header: "MAXIMUM_ANNUAL_BENEFIT", key: "MAXIMUM_ANNUAL_BENEFIT" },
    { header: "POLICYHOLDER_NAME", key: "POLICYHOLDER_NAME" },
    { header: "NATIONALITY_COUNTRY_CODE", key: "NATIONALITY_COUNTRY_CODE" },
    { header: "IDENTITY_DOCUMENT_TYPE", key: "IDENTITY_DOCUMENT_TYPE" },
    { header: "IDENTITY_DOCUMENT_ID", key: "IDENTITY_DOCUMENT_ID" },
    { header: "SEX", key: "SEX" },
    { header: "DATE_OF_BIRTH", key: "DATE_OF_BIRTH" },
    { header: "ADDRESS", key: "ADDRESS" },
    { header: "EMAIL_ADDRESS", key: "EMAIL_ADDRESS" },
    { header: "PHONE_NUMBER", key: "PHONE_NUMBER" },
    { header: "EFFECTIVE_DATE", key: "EFFECTIVE_DATE" },
    { header: "POLICYHOLDER_OWNER", key: "POLICYHOLDER_OWNER" },
    { header: "KINSHIP", key: "KINSHIP" },
    {
      header: "PRE_EXISTING_EXCLUSION_PERIOD",
      key: "PRE_EXISTING_EXCLUSION_PERIOD",
    },
    { header: "CURRENCY_CODE", key: "CURRENCY_CODE" },
    { header: "REIMBURSEMENT_BASE_AMOUNT", key: "REIMBURSEMENT_BASE_AMOUNT" },
    { header: "MAXIMUM_OUT_OF_POCKET", key: "MAXIMUM_OUT_OF_POCKET" },
    {
      header: "MAXIMUM_OUT_OF_POCKET_REACHED",
      key: "MAXIMUM_OUT_OF_POCKET_REACHED",
    },
    {
      header: "OUTPATIENT_CARE_AVAILABILITY",
      key: "OUTPATIENT_CARE_AVAILABILITY",
    },
    { header: "OUTPATIENT_CARE_LIMIT", key: "OUTPATIENT_CARE_LIMIT" },
    { header: "NET_PREMIUM_AMOUNT", key: "NET_PREMIUM_AMOUNT" },
  ];

  // Agregar filas
  result.rows.forEach((row) => {
    worksheet.addRow({
      INSURER_SLUG: "mx-mutuus",
      POLICY_NUMBER: "",
      GROUP_CODE: "",
      POLICYHOLDER_CERTIFICATE: row.MembresiaID_Modificado,
      INSURED_ID: row.MembresiaID_Modificado,
      STATUS: "ENABLED",
      MAXIMUM_LIFETIME_BENEFIT: 0,
      MAXIMUM_ANNUAL_BENEFIT: row.MontoProducto || 0,
      POLICYHOLDER_NAME: row.NombreCompleto,
      NATIONALITY_COUNTRY_CODE: "MX",
      IDENTITY_DOCUMENT_TYPE: "TAX_IDENTIFICATION_NUMBER",
      IDENTITY_DOCUMENT_ID: row.Identificador || 0,
      SEX: row.GeneroClave === "M" ? "MALE" : "FEMALE",
      DATE_OF_BIRTH: row.FechaNacimiento,
      ADDRESS: row.DomicilioCompleto,
      EMAIL_ADDRESS: row.Email,
      PHONE_NUMBER: row.NumeroTelefono,
      EFFECTIVE_DATE: row.FechaDesde,
      POLICYHOLDER_OWNER: row.OrdenTipo === 0 ? "TRUE" : "FALSE",
      KINSHIP: row.OrdenTipo === 1 ? "CHILD" : "",
      PRE_EXISTING_EXCLUSION_PERIOD: 180,
      CURRENCY_CODE: "MXN",
      REIMBURSEMENT_BASE_AMOUNT: 0,
      MAXIMUM_OUT_OF_POCKET: row.MontoProducto || 0,
      MAXIMUM_OUT_OF_POCKET_REACHED: "FALSE",
      OUTPATIENT_CARE_AVAILABILITY: "UNLIMITED",
      OUTPATIENT_CARE_LIMIT: "",
      NET_PREMIUM_AMOUNT: 0,
    });
  });

  const filePath = path.join(
    path.dirname(inputFilePath),
    "MembresiasConsulta2.xlsx"
  );
  await workbook.xlsx.writeFile(filePath);

  console.log(`Archivo generado: ${filePath}`);

  await client.end();
};

const generateExcelFilE2 = async (inputFilePath) => {
  const client = new Client({
    host: "dbmutuusv2prod.postgres.database.azure.com",
    database: "Mutuus",
    user: "mutuusreportes",
    password: "Mu7uus%Pr0%",
    port: 5432,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  const query = `
    WITH MembresiaConAsociado AS (
        SELECT 
            m."ID"::TEXT AS "MembresiaID_Modificado",
            m."Saldo",
            0 AS "OrdenTipo",
            CONCAT(a."ApellidoParterno", ' ', a."ApellidoMaterno", ' ', a."Nombre") AS "NombreCompleto",
            a."Rfc" AS "Identificador",
            a."GeneroClave",
            a."FechaNacimiento",
            a."Email",
            a."Celular" AS "NumeroTelefono",
            m."FechaDesde",
            p."MontoProducto" AS "MontoProducto",
            CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
        FROM 
            public."Membresias" m
        INNER JOIN 
            public."Asociados" a ON m."AsociadoID" = a."ID"
        INNER JOIN 
            public."Productos" p ON m."ProductoID" = p."ID"
        LEFT JOIN 
            public."Domicilio" d ON a."DomicilioID" = d."ID"
        LEFT JOIN 
            public."Colonias" c ON d."ColoniaID" = c."ID"
        LEFT JOIN 
            public."Estados" e ON c."MunicipioID" = e."ID"
        WHERE 
            m."Estatus" = 1
    ),
    BeneficiariosConMembresia AS (
        SELECT 
            m."ID"::TEXT || '-' || ROW_NUMBER() OVER (PARTITION BY m."ID" ORDER BY b."ID") AS "MembresiaID_Modificado",
            b."Saldo", 
            1 AS "OrdenTipo",
            CONCAT(b."ApellidoPaterno", ' ', b."ApellidoMaterno", ' ', b."Nombre") AS "NombreCompleto",
            b."Curp" AS "Identificador",
            b."GeneroClave",
            b."FechaNacimiento",
            a."Email",
            a."Celular" AS "NumeroTelefono",
            m."FechaDesde",
            p."MontoProducto" AS "MontoProducto",
            CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
        FROM 
            public."Membresias" m
        INNER JOIN 
            public."Asociados" a ON m."AsociadoID" = a."ID"
        INNER JOIN 
            public."Beneficiarios" b ON a."ID" = b."AsociadoID"
        INNER JOIN 
            public."Productos" p ON m."ProductoID" = p."ID"
        LEFT JOIN 
            public."Domicilio" d ON a."DomicilioID" = d."ID"
        LEFT JOIN 
            public."Colonias" c ON d."ColoniaID" = c."ID"
        LEFT JOIN 
            public."Estados" e ON c."MunicipioID" = e."ID"
        WHERE 
            m."Estatus" = 1
    )
    SELECT 
        *
    FROM (
        SELECT * FROM MembresiaConAsociado
        UNION ALL
        SELECT * FROM BeneficiariosConMembresia
    ) AS Resultado
    ORDER BY 
        "OrdenTipo",
        (string_to_array("MembresiaID_Modificado", '-')::int[])[1], 
        (CASE 
            WHEN "MembresiaID_Modificado" LIKE '%-%' THEN (string_to_array("MembresiaID_Modificado", '-')::int[])[2] 
            ELSE NULL 
        END);
    `;

  const result = await client.query(query);

  // Función para normalizar la longitud y eliminar sufijo
  const normalizeValue = (value, length) => {
    return value.toString().split("-")[0].padStart(length, "0"); // Remover el sufijo '-1' y normalizar longitud
  };

  // Leer el archivo Excel con las pólizas
  const inputWorkbook = new ExcelJS.Workbook();
  await inputWorkbook.xlsx.readFile(inputFilePath);
  const policiesWorksheet = inputWorkbook.getWorksheet(1);
  const policies = {};

  policiesWorksheet.eachRow((row, rowNumber) => {
    if (rowNumber > 1) {
      // Saltar encabezado
      const certificado = row.getCell(1).value
        ? normalizeValue(row.getCell(1).value.toString().trim(), 8)
        : null; // Normalizar
      const poliza = row.getCell(2).value
        ? `M${normalizeValue(row.getCell(2).value.toString().trim(), 7)}`
        : null; // Normalizar
      if (certificado && poliza) {
        policies[certificado] = poliza;
      }
    }
  });

  // Crear un nuevo archivo Excel para guardar los resultados
  const outputWorkbook = new ExcelJS.Workbook();
  const worksheet = outputWorkbook.addWorksheet("v2_policyholders");

  worksheet.columns = [
    { header: "INSURER_SLUG", key: "INSURER_SLUG" },
    { header: "POLICY_NUMBER", key: "POLICY_NUMBER" },
    { header: "GROUP_CODE", key: "GROUP_CODE" },
    { header: "POLICYHOLDER_CERTIFICATE", key: "POLICYHOLDER_CERTIFICATE" },
    { header: "INSURED_ID", key: "INSURED_ID" },
    { header: "STATUS", key: "STATUS" },
    { header: "MAXIMUM_LIFETIME_BENEFIT", key: "MAXIMUM_LIFETIME_BENEFIT" },
    { header: "MAXIMUM_ANNUAL_BENEFIT", key: "MAXIMUM_ANNUAL_BENEFIT" },
    { header: "POLICYHOLDER_NAME", key: "POLICYHOLDER_NAME" },
    { header: "NATIONALITY_COUNTRY_CODE", key: "NATIONALITY_COUNTRY_CODE" },
    { header: "IDENTITY_DOCUMENT_TYPE", key: "IDENTITY_DOCUMENT_TYPE" },
    { header: "IDENTITY_DOCUMENT_ID", key: "IDENTITY_DOCUMENT_ID" },
    { header: "SEX", key: "SEX" },
    { header: "DATE_OF_BIRTH", key: "DATE_OF_BIRTH" },
    { header: "ADDRESS", key: "ADDRESS" },
    { header: "EMAIL_ADDRESS", key: "EMAIL_ADDRESS" },
    { header: "PHONE_NUMBER", key: "PHONE_NUMBER" },
    { header: "EFFECTIVE_DATE", key: "EFFECTIVE_DATE" },
    { header: "POLICYHOLDER_OWNER", key: "POLICYHOLDER_OWNER" },
    { header: "KINSHIP", key: "KINSHIP" },
    {
      header: "PRE_EXISTING_EXCLUSION_PERIOD",
      key: "PRE_EXISTING_EXCLUSION_PERIOD",
    },
    { header: "CURRENCY_CODE", key: "CURRENCY_CODE" },
    { header: "REIMBURSEMENT_BASE_AMOUNT", key: "REIMBURSEMENT_BASE_AMOUNT" },
    { header: "MAXIMUM_OUT_OF_POCKET", key: "MAXIMUM_OUT_OF_POCKET" },
    {
      header: "MAXIMUM_OUT_OF_POCKET_REACHED",
      key: "MAXIMUM_OUT_OF_POCKET_REACHED",
    },
    {
      header: "OUTPATIENT_CARE_AVAILABILITY",
      key: "OUTPATIENT_CARE_AVAILABILITY",
    },
    { header: "OUTPATIENT_CARE_LIMIT", key: "OUTPATIENT_CARE_LIMIT" },
    { header: "NET_PREMIUM_AMOUNT", key: "NET_PREMIUM_AMOUNT" },
  ];

  result.rows.forEach((row) => {
    const membershipId = normalizeValue(row.MembresiaID_Modificado.trim(), 8); // Normalizar el ID eliminando sufijos
    const policyNumber = policies[membershipId] || "M0000001"; // Verificar si existe en el mapa
    const groupCode = `${policyNumber}-1`;

    worksheet.addRow({
      INSURER_SLUG: "mx-mutuus",
      POLICY_NUMBER: policyNumber,
      GROUP_CODE: groupCode,
      POLICYHOLDER_CERTIFICATE: row.MembresiaID_Modificado,
      INSURED_ID: row.MembresiaID_Modificado,
      STATUS: "ENABLED",
      MAXIMUM_LIFETIME_BENEFIT: 0,
      MAXIMUM_ANNUAL_BENEFIT: row.MontoProducto || 0,
      POLICYHOLDER_NAME: row.NombreCompleto,
      NATIONALITY_COUNTRY_CODE: "MX",
      IDENTITY_DOCUMENT_TYPE: "TAX_IDENTIFICATION_NUMBER",
      IDENTITY_DOCUMENT_ID: row.Identificador || 0,
      SEX: row.GeneroClave === "M" ? "MALE" : "FEMALE",
      DATE_OF_BIRTH: row.FechaNacimiento,
      ADDRESS: row.DomicilioCompleto,
      EMAIL_ADDRESS: row.Email,
      PHONE_NUMBER: row.NumeroTelefono,
      EFFECTIVE_DATE: row.FechaDesde,
      POLICYHOLDER_OWNER: row.OrdenTipo === 0 ? "TRUE" : "FALSE",
      KINSHIP: row.OrdenTipo === 1 ? "CHILD" : "",
      PRE_EXISTING_EXCLUSION_PERIOD: 180,
      CURRENCY_CODE: "MXN",
      REIMBURSEMENT_BASE_AMOUNT: 0,
      MAXIMUM_OUT_OF_POCKET: row.MontoProducto || 0,
      MAXIMUM_OUT_OF_POCKET_REACHED: "FALSE",
      OUTPATIENT_CARE_AVAILABILITY: "UNLIMITED",
      OUTPATIENT_CARE_LIMIT: "",
      NET_PREMIUM_AMOUNT: 0,
    });
  });

  const filePath = path.join(
    path.dirname(inputFilePath),
    "v2_policyholders.xlsx"
  );
  await outputWorkbook.xlsx.writeFile(filePath);

  console.log(`Archivo generado: ${filePath}`);
  await client.end();
};

const generateCsvFile = async (inputFilePath) => {
  const client = new Client({
    host: "dbmutuusv2prod.postgres.database.azure.com",
    database: "Mutuus",
    user: "mutuusreportes",
    password: "Mu7uus%Pr0%",
    port: 5432,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  const query = `
  WITH MembresiaConAsociado AS (
    SELECT 
        m."ID"::TEXT AS "MembresiaID_Modificado",
        m."Saldo",
        0 AS "OrdenTipo",
        CONCAT(a."ApellidoParterno", ' ', a."ApellidoMaterno", ' ', a."Nombre") AS "NombreCompleto",
        a."Rfc" AS "Identificador",
        a."GeneroClave",
        a."FechaNacimiento",
        a."Email",
        a."Celular" AS "NumeroTelefono",
        m."FechaRegistro",
        p."MontoProducto" AS "MontoProducto",
        p."ID" AS "ProductoID", -- Agregar el ID del producto
        CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
    FROM 
        public."Membresias" m
    INNER JOIN 
        public."Asociados" a ON m."AsociadoID" = a."ID"
    INNER JOIN 
        public."Productos" p ON m."ProductoID" = p."ID"
    LEFT JOIN 
        public."Domicilio" d ON a."DomicilioID" = d."ID"
    LEFT JOIN 
        public."Colonias" c ON d."ColoniaID" = c."ID"
    LEFT JOIN 
        public."Estados" e ON c."MunicipioID" = e."ID"
    WHERE 
        m."Estatus" = 1 AND p."ID" <> 5 -- Excluir el producto con ID 5
),
BeneficiariosConMembresia AS (
    SELECT 
        m."ID"::TEXT || '-' || ROW_NUMBER() OVER (PARTITION BY m."ID" ORDER BY b."ID") AS "MembresiaID_Modificado",
        b."Saldo", 
        1 AS "OrdenTipo",
        CONCAT(b."ApellidoPaterno", ' ', b."ApellidoMaterno", ' ', b."Nombre") AS "NombreCompleto",
        b."Curp" AS "Identificador",
        b."GeneroClave",
        b."FechaNacimiento",
        a."Email",
        a."Celular" AS "NumeroTelefono",
        m."FechaRegistro",
        p."MontoProducto" AS "MontoProducto",
        p."ID" AS "ProductoID", -- Agregar el ID del producto
        CONCAT(d."Calle", ' ', COALESCE(d."NumeroExterior", ''), ', ', c."ColoniaNombre", ', ', e."EstadoNombre") AS "DomicilioCompleto"
    FROM 
        public."Membresias" m
    INNER JOIN 
        public."Asociados" a ON m."AsociadoID" = a."ID"
    INNER JOIN 
        public."Beneficiarios" b ON a."ID" = b."AsociadoID"
    INNER JOIN 
        public."Productos" p ON m."ProductoID" = p."ID"
    LEFT JOIN 
        public."Domicilio" d ON a."DomicilioID" = d."ID"
    LEFT JOIN 
        public."Colonias" c ON d."ColoniaID" = c."ID"
    LEFT JOIN 
        public."Estados" e ON c."MunicipioID" = e."ID"
    WHERE 
        m."Estatus" = 1 AND p."ID" <> 5 -- Excluir el producto con ID 5
)
SELECT 
    *
FROM (
    SELECT * FROM MembresiaConAsociado
    UNION ALL
    SELECT * FROM BeneficiariosConMembresia
) AS Resultado
ORDER BY 
    "OrdenTipo",
    (string_to_array("MembresiaID_Modificado", '-')::int[])[1], 
    (CASE 
        WHEN "MembresiaID_Modificado" LIKE '%-%' THEN (string_to_array("MembresiaID_Modificado", '-')::int[])[2] 
        ELSE NULL 
    END);
  `;

  const result = await client.query(query);

  const inputWorkbook = new ExcelJS.Workbook();
  await inputWorkbook.xlsx.readFile(inputFilePath);
  const policiesWorksheet = inputWorkbook.getWorksheet(1);
  const policies = {};

  const normalizeValue = (value, length) => {
    return value.toString().split("-")[0].padStart(length, "0");
  };

  policiesWorksheet.eachRow((row, rowNumber) => {
    if (rowNumber > 1) {
      // Saltar encabezado
      const certificado = row.getCell(1).value
        ? row.getCell(1).value.toString().trim().padStart(8, "0")
        : null;
      const poliza = row.getCell(2).value
        ? `M${row.getCell(2).value.toString().trim().padStart(7, "0")}`
        : null;
      if (certificado && poliza) {
        policies[certificado] = poliza;
      }
    }
  });

  const headers = [
    "INSURER_SLUG",
    "POLICY_NUMBER",
    "GROUP_CODE",
    "POLICYHOLDER_CERTIFICATE",
    "INSURED_ID",
    "STATUS",
    "MAXIMUM_LIFETIME_BENEFIT",
    "MAXIMUM_ANNUAL_BENEFIT",
    "POLICYHOLDER_NAME",
    "NATIONALITY_COUNTRY_CODE",
    "IDENTITY_DOCUMENT_TYPE",
    "IDENTITY_DOCUMENT_ID",
    "SEX",
    "DATE_OF_BIRTH",
    "ADDRESS",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "EFFECTIVE_DATE",
    "POLICYHOLDER_OWNER",
    "KINSHIP",
    "PRE_EXISTING_EXCLUSION_PERIOD",
    "CURRENCY_CODE",
    "REIMBURSEMENT_BASE_AMOUNT",
    "MAXIMUM_OUT_OF_POCKET",
    "MAXIMUM_OUT_OF_POCKET_REACHED",
    "OUTPATIENT_CARE_AVAILABILITY",
    "OUTPATIENT_CARE_LIMIT",
    "OUTPATIENT_CARE_AVAILABLE",
    "NET_PREMIUM_AMOUNT",
  ];

  const rows = result.rows.map((row) => {
    const membershipId = normalizeValue(row.MembresiaID_Modificado.trim(), 8);
    const policyNumber = policies[membershipId] || "M0000001";
    const groupCode = `${policyNumber}-1`;

    return {
      INSURER_SLUG: "mx-mutuus",
      POLICY_NUMBER: policyNumber,
      GROUP_CODE: groupCode,
      POLICYHOLDER_CERTIFICATE: row.MembresiaID_Modificado,
      INSURED_ID: row.MembresiaID_Modificado,
      STATUS: "ENABLED",
      MAXIMUM_LIFETIME_BENEFIT: 0,
      MAXIMUM_ANNUAL_BENEFIT: row.MontoProducto || 0,
      POLICYHOLDER_NAME: row.NombreCompleto,
      NATIONALITY_COUNTRY_CODE: "MX",
      IDENTITY_DOCUMENT_TYPE: "TAX_IDENTIFICATION_NUMBER",
      IDENTITY_DOCUMENT_ID: row.Identificador || 0,
      SEX: row.GeneroClave === "M" ? "MALE" : "FEMALE",
      DATE_OF_BIRTH: row.FechaNacimiento
        ? new Date(row.FechaNacimiento).toISOString()
        : "",
      ADDRESS: row.DomicilioCompleto,
      EMAIL_ADDRESS: row.Email,
      PHONE_NUMBER: row.NumeroTelefono,
      EFFECTIVE_DATE: row.FechaRegistro
        ? new Date(row.FechaRegistro).toISOString()
        : "",
      POLICYHOLDER_OWNER: row.OrdenTipo === 0 ? "TRUE" : "FALSE",
      KINSHIP: row.OrdenTipo === 1 ? "CHILD" : "",
      PRE_EXISTING_EXCLUSION_PERIOD: 180,
      CURRENCY_CODE: "MXN",
      REIMBURSEMENT_BASE_AMOUNT: 0,
      MAXIMUM_OUT_OF_POCKET: row.MontoProducto || 0,
      MAXIMUM_OUT_OF_POCKET_REACHED: "FALSE",
      OUTPATIENT_CARE_AVAILABILITY: "UNLIMITED",
      OUTPATIENT_CARE_LIMIT: "",
      OUTPATIENT_CARE_AVAILABLE:"",
      NET_PREMIUM_AMOUNT: 0,
    };
  });
  const csv = await new Promise((resolve, reject) => {
    stringify(
      rows,
      { header: true, columns: headers, quoted: true, escape: "\\" },
      (err, output) => {
        if (err) reject(err);
        else resolve(output);
      }
    );
  });
  const v2_policyholders = path.join(path.dirname(inputFilePath), "v2_policyholders.csv");
  fs.writeFileSync(v2_policyholders, csv, "utf8");
  console.log(`Archivo generado: ${v2_policyholders}`);

  await client.end();
};

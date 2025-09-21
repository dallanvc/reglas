import { Page , test } from "@playwright/test";
import PlaywrightWrapper from "../../src/helper/wrapper/PlaywrightWrappers";
import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import DatabaseConnection from "../../src/helper/wrapper/DatabaseConnection27";
import DatabaseConnection206 from "../../src/helper/wrapper/DatabaseConnection206";


export default class busquedaCRMPage {
    private base: PlaywrightWrapper;
    constructor(private page: Page
    ) {
        this.base = new PlaywrightWrapper(page);
    }

    private formatLocalDateTime(date: Date): string {
      const newDate = new Date(date.getTime());
      newDate.setHours(newDate.getHours() + 5);
      const year = newDate.getFullYear();
      const month = (newDate.getMonth() + 1).toString().padStart(2, '0');
      const day = newDate.getDate().toString().padStart(2, '0');
      const hours = newDate.getHours().toString().padStart(2, '0');
      const minutes = newDate.getMinutes().toString().padStart(2, '0');
      const seconds = newDate.getSeconds().toString().padStart(2, '0');

      return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }


    async conexionBD() {
        try {
            const pool = await DatabaseConnection.getConnection(); // Obtén la conexión desde la clase centralizada
            const query = `
                SELECT TOP (1000) [id],
                    [id_torneo],
                    [nombre_promocion],
                    [fecha_inicio_torneo],
                    [fecha_fin_torneo]
                FROM [Mesas].[dbo].[s_torneo_mesas.configuracion]
                WHERE fecha_inicio_torneo >= GETDATE() AND id_torneo = 1
            `;

            const result = await pool.request().query(query); // Ejecuta la consulta
            console.log('Resultado de la consulta:', result.recordset);
            return result.recordset; // Devuelve los resultados
        } catch (error) {
            console.error('❌ Error al ejecutar la consulta:', error);
            throw error;
        }
    }
    
    async obtenerSegmentos(mes: number, codigoPromocion: number): Promise<void> {
        const query = `
            SELECT id_ejecucion_segmento, nombre_segmento, fecha_inicio, fecha_fin, fecha_registro
            FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento]
            WHERE
                id_promocion = ${codigoPromocion}
                AND
                (
                    -- Records from the current month and year
                    MONTH(fecha_registro) = MONTH(GETDATE())
                    AND YEAR(fecha_registro) = YEAR(GETDATE())
                    OR
                    -- Records from the first 10 days of the current month
                    (
                        fecha_registro >= DATEADD(day, 0, DATEDIFF(day, 0, GETDATE()) - DAY(GETDATE()) + 1)
                        AND fecha_registro < DATEADD(day, 10, DATEDIFF(day, 0, GETDATE()) - DAY(GETDATE()) + 1)
                    )
                    OR
                    -- Records from the last 10 days of the previous month
                    (
                        fecha_registro >= DATEADD(day, -10, DATEDIFF(day, 0, GETDATE()) - DAY(GETDATE()) + 1)
                        AND fecha_registro < DATEADD(day, 0, DATEDIFF(day, 0, GETDATE()) - DAY(GETDATE()) + 1)
                    )
                );
        `;
        try {
            const pool = await DatabaseConnection.getConnection(); // Obtén la conexión desde la clase centralizada
            const result = await pool.request().query(query); // Ejecuta la consulta

            for (const row of result.recordset) {
                const {
                    id_ejecucion_segmento,
                    nombre_segmento,
                    fecha_inicio,
                    fecha_fin,
                    fecha_registro
                } = row;

                const fechaInicioFormateada = fecha_inicio ? this.formatLocalDateTime(new Date(fecha_inicio)) : null;
                const fechaFinFormateada = fecha_fin ? this.formatLocalDateTime(new Date(fecha_fin)) : null;
                const fechaRegistroFormateada = fecha_registro ? this.formatLocalDateTime(new Date(fecha_registro)) : null;


                // 📋 Log descriptivo
                const logInfo = `
                📌 **Información del Segmento**
                🆔 **ID Ejecución Segmento:** ${id_ejecucion_segmento}
                🏷️ **Nombre Segmento:** ${nombre_segmento}
                🕐 **Fecha Inicio:** ${fechaInicioFormateada}
                ⏰ **Fecha Fin:** ${fechaFinFormateada}
                🗓️ **Fecha Registro:** ${fechaRegistroFormateada}
                                `;
                console.log(logInfo);

                // Crear JSON
                const jsonObject = {
                    id_ejecucion_segmento,
                    nombre_segmento,
                    fecha_inicio: fechaInicioFormateada,
                    fecha_fin: fechaFinFormateada,
                    fecha_registro: fechaRegistroFormateada
                    };

                // Crear ruta del archivo
                const directoryPath = path.join(__dirname, `DatosBD/${codigoPromocion}`);
                const fileName = path.join(directoryPath, `segmento_${id_ejecucion_segmento}.json`);

                // Crear directorio si no existe
                if (!fs.existsSync(directoryPath)) {
                    fs.mkdirSync(directoryPath, { recursive: true });
                }

                // Guardar el archivo JSON
                try {
                    fs.writeFileSync(fileName, JSON.stringify(jsonObject, null, 4)); // Pretty print con indentación
                    console.log(`✅ Segmento guardado: ${fileName}`);
                } catch (error) {
                    console.error(`❌ Error al guardar el archivo ${fileName}`, error);
                }
            }
        } catch (error) {
            console.error('❌ Error al ejecutar la consulta:', error);
            throw error;
        }
    }
    

async extraccionConfiguraciones(codPromocion: string): Promise<void> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codPromocion);

    if (!fs.existsSync(folderPath)) {
      console.error(`❌ Carpeta no encontrada: ${folderPath}`);
      return;
    }

    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      const filePath = path.join(folderPath, file);
      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(content);
        const idSegmento = jsonData['id_ejecucion_segmento'];

        const query = `
          SELECT e.id_ejecucion_segmento, e.id_promocion, e.nombre_segmento,
                 cf.id_configuracion_detalle, cf.codigo_compuesto,
                 cf.nombre, cf.valor_entero
          FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
          JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_configuracion_detalle] cf
          ON e.id_configuracion = cf.id_configuracion
          WHERE e.id_ejecucion_segmento = ${idSegmento};
        `;

        const pool = await DatabaseConnection.getConnection(); // Obtén la conexión desde la clase centralizada
        const result = await pool.request().query(query); // Ejecuta la consulta


        if (result.recordset.length === 0) {
          console.warn(`⚠️ No se encontró configuración para el segmento ID: ${idSegmento}`);
          continue;
        }

        let count = 0;
        for (const row of result.recordset) {
          const {
            id_ejecucion_segmento,
            id_promocion,
            nombre_segmento,
            id_configuracion_detalle,
            codigo_compuesto,
            nombre,
            valor_entero
          } = row;

          const logInfo = `
          🔍 Configuración encontrada:
          🆔 Segmento: ${id_ejecucion_segmento}
          🏷️ Nombre Segmento: ${nombre_segmento}
          ⚙️ ID Config Detalle: ${id_configuracion_detalle}
          🧩 Código: ${codigo_compuesto}
          📛 Nombre: ${nombre}
          🔢 Valor Entero: ${valor_entero}
          `;
          console.log(logInfo);

          const resultJson = {
            id_ejecucion_segmento,
            id_promocion,
            nombre_segmento,
            id_configuracion_detalle,
            codigo_compuesto,
            nombre,
            valor_entero
          };

          const outputFileName = path.join(folderPath, `config_segmento_${idSegmento}_${count}.json`);
          fs.writeFileSync(outputFileName, JSON.stringify(resultJson, null, 2), 'utf-8');
          console.log(`✅ Configuración guardada: ${outputFileName}`);
          count++;
        }
      } catch (error) {
        console.error(`❌ Error procesando el archivo: ${filePath}`);
        console.error(error);
      }
    }
  }

  async extraccionEquivalenciaSegmento(codPromocion: string): Promise<void> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codPromocion);

    if (!fs.existsSync(folderPath)) {
      console.error(`❌ Carpeta no encontrada: ${folderPath}`);
      return;
    }

    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      const filePath = path.join(folderPath, file);

      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(content);
        const idSegmento = jsonData['id_ejecucion_segmento'];

        const query = `
          SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_equivalencia_Puntaje, e.nombre_segmento,
                 eq.condicion_minima, eq.condicion_maxima, eq.valor_puntaje
          FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
          JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_Equivalencia_Puntaje_Detalle] eq
          ON e.id_equivalencia_Puntaje = eq.id_equivalencia_puntaje
          WHERE e.id_ejecucion_segmento = ${idSegmento};
        `;

        const pool = await DatabaseConnection.getConnection();
        const result = await pool.request().query(query);

        if (result.recordset.length === 0) {
          console.warn(`⚠️ No se encontró equivalencia para el segmento ID: ${idSegmento}`);
          continue;
        }

        let count = 0;
        for (const row of result.recordset) {
          const {
            id_ejecucion_segmento,
            id_promocion,
            id_equivalencia_Puntaje,
            nombre_segmento,
            condicion_minima,
            condicion_maxima,
            valor_puntaje
          } = row;

          const resultJson = {
            id_ejecucion_segmento,
            id_promocion,
            id_equivalencia_Puntaje,
            nombre_segmento,
            condicion_minima,
            condicion_maxima,
            valor_puntaje
          };

          const outputFileName = path.join(folderPath, `equivalencia_segmento_${idSegmento}_${count}.json`);
          fs.writeFileSync(outputFileName, JSON.stringify(resultJson, null, 2), 'utf-8');
          console.log(`✅ Equivalencia guardada: ${outputFileName}`);
          count++;
        }
      } catch (error) {
        console.error(`❌ Error procesando el archivo: ${filePath}`);
        console.error(error);
      }
    }
  }

  async extraccionMultiplicadorSegmento(codPromocion: string): Promise<void> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codPromocion);

    if (!fs.existsSync(folderPath)) {
      console.error(`❌ Carpeta no encontrada: ${folderPath}`);
      return;
    }

    const files = fs.readdirSync(folderPath).filter(file => file.endsWith('.json'));

    for (const file of files) {
      const filePath = path.join(folderPath, file);
      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(content);
        const idSegmento = jsonData['id_ejecucion_segmento'];

        const query = `
          SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
                 e.id_calendario_multiplicador, e.id_equivalencia_Puntaje,
                 e.nombre_segmento, m.valor_multiplicador,
                 m.fecha_hora_inicio, m.fecha_hora_fin
          FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
          JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_cronograma_multiplicador] m
            ON e.id_calendario_multiplicador = m.id_calendario
          WHERE e.id_ejecucion_segmento = ${idSegmento};
        `;

        const pool = await DatabaseConnection.getConnection();
        const result = await pool.request().query(query);

        if (result.recordset.length === 0) {
          console.warn(`⚠️ No se encontró multiplicador para el segmento ID: ${idSegmento}`);
          continue;
        }

        let count = 0;
        for (const row of result.recordset) {
          const {
            id_ejecucion_segmento,
            id_promocion,
            id_premio,
            id_calendario_multiplicador,
            id_equivalencia_Puntaje,
            nombre_segmento,
            valor_multiplicador,
            fecha_hora_inicio,
            fecha_hora_fin
          } = row;

          const fechaHoraInicioFormateada = fecha_hora_inicio ? this.formatLocalDateTime(new Date(fecha_hora_inicio)) : null;
          const fechaHoraFinFormateada = fecha_hora_fin ? this.formatLocalDateTime(new Date(fecha_hora_fin)) : null;


          const resultJson = {
            id_ejecucion_segmento,
            id_promocion,
            id_premio,
            id_calendario_multiplicador,
            id_equivalencia_Puntaje,
            nombre_segmento,
            valor_multiplicador,
            fecha_hora_inicio: fechaHoraInicioFormateada,
            fecha_hora_fin: fechaHoraFinFormateada
          };

          const outputFileName = path.join(folderPath, `multiplicador_segmento_${idSegmento}_${count}.json`);
          fs.writeFileSync(outputFileName, JSON.stringify(resultJson, null, 2), 'utf-8');
          console.log(`✅ Multiplicador guardado: ${outputFileName}`);
          count++;
        }
      } catch (error) {
        console.error(`❌ Error procesando el archivo: ${filePath}`);
        console.error(error);
      }
    }
  }
async extraccionPremiosSegmento(codPromocion: string): Promise<void> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codPromocion);

    if (!fs.existsSync(folderPath)) {
      console.error(`❌ Carpeta no encontrada: ${folderPath}`);
      return;
    }

    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      const filePath = path.join(folderPath, file);

      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(content);
        const idSegmento = jsonData['id_ejecucion_segmento'];

        const query = `
          SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
                 p.condicion_minima, p.condicion_maxima, p.valor_premio, p.cantidad_ganadores
          FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
          JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_premio_detalle] p
            ON e.id_premio = p.id_premio
          WHERE e.id_ejecucion_segmento = ${idSegmento}
          ORDER BY p.valor_premio;
        `;

        const pool = await DatabaseConnection.getConnection();
        const result = await pool.request().query(query);

        if (result.recordset.length === 0) {
          console.warn(`⚠️ No se encontraron premios para el segmento ID: ${idSegmento}`);
          continue;
        }

        let count = 0;
        for (const row of result.recordset) {
          const {
            id_ejecucion_segmento,
            id_promocion,
            id_premio,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores
          } = row;

          const resultJson = {
            id_ejecucion_segmento,
            id_promocion,
            id_premio,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores
          };

          const outputFileName = path.join(folderPath, `premio_segmento_${idSegmento}_${count}.json`);
          fs.writeFileSync(outputFileName, JSON.stringify(resultJson, null, 2), 'utf-8');
          console.log(`✅ Premio guardado: ${outputFileName}`);
          count++;
        }

      } catch (error) {
        console.error(`❌ Error procesando el archivo: ${filePath}`);
        console.error(error);
      }
    }
  }

  async extraccionEtapasSegmento(codPromocion: string): Promise<void> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codPromocion);

    if (!fs.existsSync(folderPath)) {
      console.error(`❌ Carpeta no encontrada: ${folderPath}`);
      return;
    }

    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      const filePath = path.join(folderPath, file);

      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        const jsonData = JSON.parse(content);
        const idSegmento = jsonData['id_ejecucion_segmento'];

        const query = `
                SELECT 
                e.id_ejecucion_segmento, 
                e.id_promocion,
                et.fecha_inicio,
                et.fecha_fin,
                et.nombre_etapa
                FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
                JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_etapa] et
                    ON e.id_ejecucion_segmento = et.id_ejecucion_segmento
                WHERE e.id_ejecucion_segmento = ${idSegmento}
                ORDER BY et.fecha_inicio ASC; 
        `;

        const pool = await DatabaseConnection.getConnection();
        const result = await pool.request().query(query);

        if (result.recordset.length === 0) {
          console.warn(`⚠️ No se encontraron etapas para el segmento ID: ${idSegmento}`);
          continue;
        }

        let count = 0;
        for (const row of result.recordset) {
          const {
            id_ejecucion_segmento,
            id_promocion,
            fecha_inicio,
            fecha_fin,
            nombre_etapa
          } = row;

          const fechaInicioFormateada = fecha_inicio ? this.formatLocalDateTime(new Date(fecha_inicio)) : null;
          const fechaFinFormateada = fecha_fin ? this.formatLocalDateTime(new Date(fecha_fin)) : null;

          const resultJson = {
            id_ejecucion_segmento,
            id_promocion,
            fecha_inicio: fechaInicioFormateada,
            fecha_fin: fechaFinFormateada,
            nombre_etapa
          };

          const outputFileName = path.join(folderPath, `etapa_segmento_${idSegmento}_${count}.json`);
          fs.writeFileSync(outputFileName, JSON.stringify(resultJson, null, 2), 'utf-8');
          console.log(`✅ Premio guardado: ${outputFileName}`);
          count++;
        }

      } catch (error) {
        console.error(`❌ Error procesando el archivo: ${filePath}`);
        console.error(error);
      }
    }
  }

  async validarMultiplicadorSegmento(codigopromocion: string) {
    switch (codigopromocion) {
      //SOLO EL MES DE JUNIO ESTA APLICANDO LAS MISMAS FECHAS DE MULTIPLICADOR
      //EN CASO CAMBIAR ACTUALIAR LOS METODOS MUY SIMILARES PERO CAMBIANDO FECHAS SEGUN MULTIPLICADOR
        case "17":
            return this.validarMultiplicadorPromo17(codigopromocion);
        case "18":
            return this.validarMultiplicadorPromo18(codigopromocion);
        case "19":
            return this.validarMultiplicadorPromo18(codigopromocion);
        case "22":
            return this.validarMultiplicadorPromo18(codigopromocion);
        default:
            throw new Error("Promoción no soportada");
    }
}

  async validarMultiplicadorPromo17(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('multiplicador_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de multiplicador para la promoción ${codigopromocion}`);
        return;
    }

    const now = new Date();
    const mesActual = now.getMonth();
    const anioActual = now.getFullYear();

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const valorMultiplicador = data.valor_multiplicador;
        const fechaInicio = new Date(data.fecha_hora_inicio.replace(" ", "T"));
        const fechaFin = new Date(data.fecha_hora_fin.replace(" ", "T"));

        if (fechaInicio.getMonth() !== mesActual || fechaInicio.getFullYear() !== anioActual) {
            console.log(`⏩ Archivo ${file} omitido (fuera del mes/año actual: ${fechaInicio.toISOString().slice(0, 10)})`);
            return;
        }

        let estado = "OK";
        let errorMsg = "";

        if (valorMultiplicador !== 5) {
            estado = "ERROR";
            errorMsg += `Multiplicador incorrecto. Esperado: 5, Encontrado: ${valorMultiplicador}. `;
        }

        const diaSemana = fechaInicio.getDay();
        const horaInicio = fechaInicio.toTimeString().slice(0, 8);
        const horaFin = fechaFin.toTimeString().slice(0, 8);

        let valido = false;
        let detalle = '';
        let esperado = '';

        if (diaSemana === 1) {
            valido = horaInicio === "08:00:00" && horaFin === "23:59:59";
            detalle = "Lunes";
            esperado = "08:00:00 a 23:59:59";
        } else if (diaSemana === 2) {
            valido = (horaInicio === "00:00:00" && horaFin === "07:59:59") ||
                     (horaInicio === "08:00:00" && horaFin === "23:59:59");
            detalle = "Martes";
            esperado = "00:00:00 a 07:59:59 o 08:00:00 a 23:59:59";
        } else if (diaSemana === 3) {
            valido = horaInicio === "00:00:00" && horaFin === "07:59:59";
            detalle = "Miércoles";
            esperado = "00:00:00 a 07:59:59";
        } else if (diaSemana === 4) {
            valido = horaInicio === "08:00:00" && horaFin === "23:59:59";
            detalle = "Jueves";
            esperado = "08:00:00 a 23:59:59";
        } else if (diaSemana === 5) {
            valido = horaInicio === "00:00:00" && horaFin === "07:59:59";
            detalle = "Viernes";
            esperado = "00:00:00 a 07:59:59";
        } else {
            detalle = "Día no permitido";
            esperado = "Lunes, Martes, Miércoles o Jueves";
        }

        if (!valido) {
            estado = "ERROR";
            errorMsg += `Intervalo de día/hora inválido. Esperado: ${detalle} ${esperado}. Encontrado: ${data.fecha_hora_inicio} a ${data.fecha_hora_fin}. `;
        }

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Multiplicador y horario válidos (${detalle} ${esperado})`);
        }

        resultados.push({
            archivo: file,
            valor_multiplicador: valorMultiplicador,
            fecha_hora_inicio: data.fecha_hora_inicio,
            fecha_hora_fin: data.fecha_hora_fin,
            estado,
            error: errorMsg.trim()
        });
    });
    this.guardarResultadosMultiplicador(codigopromocion, resultados);
  }

  async validarMultiplicadorPromo18(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('multiplicador_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de multiplicador para la promoción ${codigopromocion}`);
        return;
    }

    const now = new Date();
    const mesActual = now.getMonth(); // 0-11
    const anioActual = now.getFullYear();

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const valorMultiplicador = data.valor_multiplicador;
        const fechaInicio = new Date(data.fecha_hora_inicio.replace(" ", "T"));
        const fechaFin = new Date(data.fecha_hora_fin.replace(" ", "T"));

        // Solo validar si el archivo es del mes y año actual
        if (fechaInicio.getMonth() !== mesActual || fechaInicio.getFullYear() !== anioActual) {
            console.log(`⏩ Archivo ${file} omitido (fuera del mes/año actual: ${fechaInicio.toISOString().slice(0, 10)})`);
            return;
        }

        let estado = "OK";
        let errorMsg = "";

        // Validar multiplicador
        if (valorMultiplicador !== 5) {
            estado = "ERROR";
            errorMsg += `Multiplicador incorrecto. Esperado: 5, Encontrado: ${valorMultiplicador}. `;
        }

        // Validar día y hora
        const diaSemana = fechaInicio.getDay(); // 0=Domingo, 1=Lunes, ..., 6=Sábado
        const horaInicio = fechaInicio.toTimeString().slice(0, 8);
        const horaFin = fechaFin.toTimeString().slice(0, 8);

        let valido = false;
        let detalle = '';
        let esperado = '';

        if (diaSemana === 2) { // Martes
            valido = (horaInicio === "08:00:00" && horaFin === "23:59:59");
            detalle = "Martes";
            esperado = "08:00:00 a 23:59:59";
        } else if (diaSemana === 3) { // Miércoles
            valido = horaInicio === "00:00:00" && horaFin === "07:59:59";
            detalle = "Miércoles";
            esperado = "00:00:00 a 07:59:59";
        } else if (diaSemana === 4) { // Jueves
            valido = (horaInicio === "08:00:00" && horaFin === "23:59:59");
            detalle = "Jueves";
            esperado = "08:00:00 a 23:59:59";
        } else if (diaSemana === 5) { // Viernes
            valido = horaInicio === "00:00:00" && horaFin === "07:59:59";
            detalle = "Viernes";
            esperado = "00:00:00 a 07:59:59";
        } else {
            detalle = "Día no permitido";
            esperado = "Martes, Miércoles, Jueves o Viernes";
        }

        if (!valido) {
            estado = "ERROR";
            errorMsg += `Intervalo de día/hora inválido. Esperado: ${detalle} ${esperado}. Encontrado: ${data.fecha_hora_inicio} a ${data.fecha_hora_fin}. `;
        }

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Multiplicador y horario válidos (${detalle} ${esperado})`);
        }

        resultados.push({
            archivo: file,
            valor_multiplicador: valorMultiplicador,
            fecha_hora_inicio: data.fecha_hora_inicio,
            fecha_hora_fin: data.fecha_hora_fin,
            estado,
            error: errorMsg.trim()
        });
    });
    this.guardarResultadosMultiplicador(codigopromocion, resultados);
}
async guardarResultadosMultiplicador(codigopromocion: string, resultados: any[]) {
      const folderPath = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
      if (!fs.existsSync(folderPath)) {
          fs.mkdirSync(folderPath, { recursive: true });
      }
      const filePath = path.join(folderPath, 'validacion_multiplicador.json');
      fs.writeFileSync(filePath, JSON.stringify(resultados, null, 2), 'utf-8');
      console.log(`✅ Archivo de validación de multiplicador generado en: ${filePath}`);
  }


 async validarequivalenciasSegmento(codigopromocion: string, minimo: string, maximo: string, puntaje: string) {
    switch (codigopromocion) {
        case "18":
            return this.validarEquivalenciaSegmentoGenerico(codigopromocion,Number(minimo),Number(maximo),Number(puntaje));
        case "19":
            return this.validarEquivalenciaSegmentoGenerico(codigopromocion,Number(minimo),Number(maximo),Number(puntaje));
        case "22":
            return this.validarEquivalenciasSegmentoSaltaGana(codigopromocion);        
        default:
            throw new Error("Promoción no soportada");
    }
  }

  async validarEquivalenciaSegmentoGenerico(
    codigopromocion: string,
    condicionMinimaEsperada: number,
    condicionMaximaEsperada: number,
    valorPuntajeEsperado: number
) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('equivalencia_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de equivalencia para la promoción ${codigopromocion}`);
        return;
    }

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_puntaje } = data;

        let estado = "OK";
        let errorMsg = "";

        if (condicion_minima !== condicionMinimaEsperada) {
            estado = "ERROR";
            errorMsg += `condicion_minima incorrecta. Esperado: ${condicionMinimaEsperada}, Encontrado: ${condicion_minima}. `;
        }
 
        if (valor_puntaje !== valorPuntajeEsperado) {
            estado = "ERROR";
            errorMsg += `valor_puntaje incorrecto. Esperado: ${valorPuntajeEsperado}, Encontrado: ${valor_puntaje}. `;
        }

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Equivalencia válida para promoción ${codigopromocion}`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            valor_puntaje,
            estado,
            error: errorMsg.trim()
        });
    });

    this.guardarResultadosEquivalencias(codigopromocion, resultados);
}
  
async validarEquivalenciasSegmentoSaltaGana(
    codigopromocion: string
) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('equivalencia_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de equivalencia para la promoción ${codigopromocion}`);
        return;
    }

    const escalas = [
        { min: 500,   puntaje: 5 },
        { min: 750,   puntaje: 10 },
        { min: 1000,  puntaje: 20 },
        { min: 1250,  puntaje: 50 },
        { min: 1500,  puntaje: 75 },
        { min: 1750,  puntaje: 100 },
        { min: 2000,  puntaje: 125 },
        { min: 2250,  puntaje: 150 },
        { min: 2500,  puntaje: 200 },
        { min: 2501,  puntaje: 1 }
    ];

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_puntaje } = data;

        let estado = "OK";
        let errorMsg = "";

        const idx = escalas.findIndex(e => e.min === condicion_minima);
        if (idx === -1) {
            estado = "ERROR";
            errorMsg += `No se encontró escala para el mínimo ${condicion_minima}. `;
        }

        let esperadoMax: number = 0;
        let esperadoPuntaje: number = 0;
        if (estado === "OK") {
            esperadoPuntaje = escalas[idx].puntaje;
            if (condicion_minima === 2500) {
                esperadoMax = 9999999;
                esperadoPuntaje = 200;
            } else if (condicion_minima === 2501) {
                esperadoMax = 100;
                esperadoPuntaje = 1;
            } else if (idx + 1 < escalas.length) {
                esperadoMax = escalas[idx + 1].min - 1;
            } else {
                estado = "ERROR";
                errorMsg += `No hay escala siguiente para calcular el máximo de ${condicion_minima}. `;
            }
        }

        if (estado === "OK" && condicion_minima !== escalas[idx].min) {
            estado = "ERROR";
            errorMsg += `condicion_minima incorrecta. Esperado: ${escalas[idx].min}, Encontrado: ${condicion_minima}. `;
        }
        if (estado === "OK" && valor_puntaje !== esperadoPuntaje) {
            estado = "ERROR";
            errorMsg += `valor_puntaje incorrecto. Esperado: ${esperadoPuntaje}, Encontrado: ${valor_puntaje}. `;
        }

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Equivalencia válida para promoción 22`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            valor_puntaje,
            estado,
            error: errorMsg.trim()
        });
    });

    this.guardarResultadosEquivalencias(codigopromocion, resultados);
  }

  async guardarResultadosEquivalencias(codigopromocion: string, resultados: any[]) {
      const folderPath = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
      if (!fs.existsSync(folderPath)) {
          fs.mkdirSync(folderPath, { recursive: true });
      }
      const filePath = path.join(folderPath, 'validacion_equivalencias.json');
      fs.writeFileSync(filePath, JSON.stringify(resultados, null, 2), 'utf-8');
      console.log(`✅ Archivo de validación de equivalencias generado en: ${filePath}`);
  }

  async validarConfiguracionesSegmento(codigopromocion: string) {
    switch (codigopromocion) {
        case "17":
            return this.validarConfiguracionesPromo17(codigopromocion);
        case "18":
            return this.validarConfiguracionesPromo18(codigopromocion);
        case "19":
            return this.validarConfiguracionesPromo19(codigopromocion);
        case "22":
            return this.validarConfiguracionesPromo22(codigopromocion);
        // Aquí puedes agregar más promociones en el futuro
        default:
            throw new Error("Promoción no soportada para validación de configuraciones");
    }
}

async validarConfiguracionesPromo17(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('config_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        throw new Error(`No se encontraron archivos de configuración para la promoción ${codigopromocion}`);
    }

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { codigo_compuesto, valor_entero, nombre_segmento } = data;

        let esperado: number | null = null;
        let descripcion = '';
        let estado = "OK";

        if (codigo_compuesto === "CANTIDAD_FILAS") {
            esperado = 30;
            descripcion = "Cantidad de tabla de ganadores";
        } else if (codigo_compuesto === "TIPO_AUDIENCIA") {
            esperado = 1;
            descripcion = "Tipo de Audiencia";
        } else {
            // Si hay otros códigos, los puedes ignorar o loguear como no validados
            return;
        }

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Segmento: ${nombre_segmento}`);
        console.log(`   - ${descripcion} actual: ${valor_entero} | valor esperado: ${esperado}`);

        if (valor_entero !== esperado) {
            estado = "ERROR";
            console.error(`❌ Archivo ${file}: ${descripcion} incorrecto. Esperado: ${esperado}, Encontrado: ${valor_entero}`);
        } else {
            console.log(`✅ Archivo ${file}: ${descripcion} válido`);
        }

        resultados.push({
            archivo: file,
            segmento: nombre_segmento,
            codigo_compuesto,
            descripcion,
            valor: valor_entero,
            valor_esperado: esperado,
            estado
        });
    });

    this.guardarResultadosConfiguraciones(codigopromocion, resultados);
}

async validarConfiguracionesPromo18(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('config_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de configuración para la promoción ${codigopromocion}`);
        return;
    }

    const parametros = {
        "ACUM_MAX_OPC": { esperado: 200, descripcion: "Cantidad máxima de opciones a acumular" },
        "MAE_CANTIDAD_CHOCOLATEO": { esperado: 60, descripcion: "Valor Cantidad Chocolateo" },
        "OPC_MIN_CANJE": { esperado: 25, descripcion: "Cantidad mínima de opciones para poder canjear" },
        "TIPO_AUDIENCIA": { esperado: 1, descripcion: "Tipo de Audiencia" }
    };

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { codigo_compuesto, valor_entero, nombre_segmento } = data;

        if (!parametros[codigo_compuesto]) {
            // Ignora otros parámetros
            return;
        }

        const { esperado, descripcion } = parametros[codigo_compuesto];
        let estado = "OK";

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Segmento: ${nombre_segmento}`);
        console.log(`   - ${descripcion} actual: ${valor_entero} | valor esperado: ${esperado}`);

        if (valor_entero !== esperado) {
            estado = "ERROR";
            console.error(`❌ Archivo ${file}: ${descripcion} incorrecto. Esperado: ${esperado}, Encontrado: ${valor_entero}`);
        } else {
            console.log(`✅ Archivo ${file}: ${descripcion} válido`);
        }

        resultados.push({
            archivo: file,
            segmento: nombre_segmento,
            codigo_compuesto,
            descripcion,
            valor: valor_entero,
            valor_esperado: esperado,
            estado
        });
    });

    this.guardarResultadosConfiguraciones(codigopromocion, resultados);
}

async validarConfiguracionesPromo19(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('config_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de configuración para la promoción ${codigopromocion}`);
        return;
    }

    const parametros = {
        "ACUM_MAX_OPC": { esperado: 8000, descripcion: "Cantidad máxima de opciones a acumular" },
        "MAE_CANTIDAD_CHOCOLATEO": { esperado: 60, descripcion: "Valor Cantidad Chocolateo" },
        "OPC_MIN_CANJE": { esperado: 100, descripcion: "Cantidad mínima de opciones para poder canjear" },
        "TIPO_AUDIENCIA": { esperado: 1, descripcion: "Tipo de Audiencia" }
    };

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { codigo_compuesto, valor_entero, nombre_segmento } = data;

        if (!parametros[codigo_compuesto]) {
            // Ignora otros parámetros
            return;
        }

        const { esperado, descripcion } = parametros[codigo_compuesto];
        let estado = "OK";

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Segmento: ${nombre_segmento}`);
        console.log(`   - ${descripcion} actual: ${valor_entero} | valor esperado: ${esperado}`);

        if (valor_entero !== esperado) {
            estado = "ERROR";
            console.error(`❌ Archivo ${file}: ${descripcion} incorrecto. Esperado: ${esperado}, Encontrado: ${valor_entero}`);
        } else {
            console.log(`✅ Archivo ${file}: ${descripcion} válido`);
        }

        resultados.push({
            archivo: file,
            segmento: nombre_segmento,
            codigo_compuesto,
            descripcion,
            valor: valor_entero,
            valor_esperado: esperado,
            estado
        });
    });

    this.guardarResultadosConfiguraciones(codigopromocion, resultados);
}

async validarConfiguracionesPromo22(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('config_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de configuración para la promoción ${codigopromocion}`);
        return;
    }

    const parametros = {
        "ACUM_MAX_OPC": { esperado: 500, descripcion: "Cantidad máxima de opciones a acumular" },
        "ACUM_MIN_PUNTOS": { esperado: 500, descripcion: "Acumulación mínima de puntos" },
        "MAE_CANTIDAD_CHOCOLATEO": { esperado: 60, descripcion: "Valor Cantidad Chocolateo" },
        "OPC_MIN_CANJE": { esperado: 5, descripcion: "Cantidad mínima de opciones para poder canjear" },
        "TIPO_AUDIENCIA": { esperado: 1, descripcion: "Tipo de Audiencia" }
    };

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { codigo_compuesto, valor_entero, nombre_segmento } = data;

        if (!parametros[codigo_compuesto]) {
            // Ignora otros parámetros
            return;
        }

        const { esperado, descripcion } = parametros[codigo_compuesto];
        let estado = "OK";

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Segmento: ${nombre_segmento}`);
        console.log(`   - ${descripcion} actual: ${valor_entero} | valor esperado: ${esperado}`);

        if (valor_entero !== esperado) {
            estado = "ERROR";
            console.error(`❌ Archivo ${file}: ${descripcion} incorrecto. Esperado: ${esperado}, Encontrado: ${valor_entero}`);
        } else {
            console.log(`✅ Archivo ${file}: ${descripcion} válido`);
        }

        resultados.push({
            archivo: file,
            segmento: nombre_segmento,
            codigo_compuesto,
            descripcion,
            valor: valor_entero,
            valor_esperado: esperado,
            estado
        });
    });

    this.guardarResultadosConfiguraciones(codigopromocion, resultados);
}

async guardarResultadosConfiguraciones(codigopromocion: string, resultados: any[]) {
    const folderPath = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
    if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath, { recursive: true });
    }
    const filePath = path.join(folderPath, 'validacion_configuraciones.json');
    fs.writeFileSync(filePath, JSON.stringify(resultados, null, 2), 'utf-8');
    console.log(`✅ Archivo de validación de configuraciones generado en: ${filePath}`);
}


async validarPremiosSegmento(codigopromocion: string) {
    switch (codigopromocion) {
        case "17":
            return this.validarPremiosPromo17(codigopromocion);
        case "18":
            return this.validarPremiosPromo18(codigopromocion);
        case "19":
            return this.validarPremiosPromo19(codigopromocion);
        case "22":
            return this.validarPremiosPromo22(codigopromocion);
        default:
            throw new Error("Promoción no soportada para validación de premios");
    }
}

async validarPremiosPromo17(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('premio_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de premios para la promoción ${codigopromocion}`);
        return;
    }

    // Tabla de premios esperados para promo 17
    const premiosEsperados = [
        { condicion_minima: 1,  condicion_maxima: 0, valor_premio: 10000, cantidad_ganadores: 1 },
        { condicion_minima: 2,  condicion_maxima: 0, valor_premio: 5500,  cantidad_ganadores: 1 },
        { condicion_minima: 3,  condicion_maxima: 0, valor_premio: 3500,  cantidad_ganadores: 1 },
        { condicion_minima: 4,  condicion_maxima: 0, valor_premio: 2000,  cantidad_ganadores: 1 },
        { condicion_minima: 5,  condicion_maxima: 0, valor_premio: 1500,  cantidad_ganadores: 1 },
        { condicion_minima: 6,  condicion_maxima: 0, valor_premio: 1000,  cantidad_ganadores: 1 },
        { condicion_minima: 7,  condicion_maxima: 0, valor_premio: 800,   cantidad_ganadores: 1 },
        { condicion_minima: 8,  condicion_maxima: 0, valor_premio: 700,   cantidad_ganadores: 1 },
        { condicion_minima: 9,  condicion_maxima: 0, valor_premio: 600,   cantidad_ganadores: 1 },
        { condicion_minima: 10, condicion_maxima: 0, valor_premio: 500,   cantidad_ganadores: 1 },
        { condicion_minima: 11, condicion_maxima: 20, valor_premio: 400,  cantidad_ganadores: 10 },
        { condicion_minima: 21, condicion_maxima: 25, valor_premio: 300,  cantidad_ganadores: 5 }
    ];

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_premio, cantidad_ganadores } = data;

        // Buscar el premio esperado según el intervalo y valor
        const esperado = premiosEsperados.find(p =>
            p.condicion_minima === condicion_minima &&
            p.condicion_maxima === condicion_maxima &&
            p.valor_premio === valor_premio
        );

        let estado = "OK";
        let errorMsg = "";

        if (!esperado) {
            estado = "ERROR";
            errorMsg += `Premio no esperado para el intervalo [${condicion_minima}, ${condicion_maxima}] y valor ${valor_premio}. `;
        } else if (cantidad_ganadores !== esperado.cantidad_ganadores) {
            estado = "ERROR";
            errorMsg += `Cantidad de ganadores incorrecta. Esperado: ${esperado.cantidad_ganadores}, Encontrado: ${cantidad_ganadores}. `;
        }

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Intervalo: [${condicion_minima}, ${condicion_maxima}]`);
        console.log(`   - Valor premio: ${valor_premio}`);
        console.log(`   - Cantidad ganadores: ${cantidad_ganadores}`);

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Premio válido`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores,
            estado,
            error: errorMsg.trim()
        });
    });

    await this.guardarResultadosPremios(codigopromocion, resultados);
}

async guardarResultadosPremios(codigopromocion: string, resultados: any[]) {
    const folderPath = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
    if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath, { recursive: true });
    }
    const filePath = path.join(folderPath, 'validacion_premios.json');
    fs.writeFileSync(filePath, JSON.stringify(resultados, null, 2), 'utf-8');
    console.log(`✅ Archivo de validación de premios generado en: ${filePath}`);
}

async validarPremiosPromo18(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('premio_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de premios para la promoción ${codigopromocion}`);
        return;
    }

    // Escenario 1: solo premios de 2 ganadores
    const escenario1 = [
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 500, cantidad_ganadores: 2 },
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 1000, cantidad_ganadores: 2 },
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 2000, cantidad_ganadores: 2 }
    ];
    // Escenario 2: premios de 2 y 1 ganador
    const escenario2 = [
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 500, cantidad_ganadores: 2 },
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 1000, cantidad_ganadores: 2 },
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 1500, cantidad_ganadores: 2 },
        { condicion_minima: 1, condicion_maxima: 0, valor_premio: 5000, cantidad_ganadores: 1 }
    ];

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_premio, cantidad_ganadores } = data;

        let estado = "OK";
        let errorMsg = "";

        // Buscar en ambos escenarios
        const esValido = escenario1.some(p =>
            p.condicion_minima === condicion_minima &&
            p.condicion_maxima === condicion_maxima &&
            p.valor_premio === valor_premio &&
            p.cantidad_ganadores === cantidad_ganadores
        ) || escenario2.some(p =>
            p.condicion_minima === condicion_minima &&
            p.condicion_maxima === condicion_maxima &&
            p.valor_premio === valor_premio &&
            p.cantidad_ganadores === cantidad_ganadores
        );

        if (!esValido) {
            estado = "ERROR";
            errorMsg += `Premio no esperado para los escenarios válidos. [condicion_minima: ${condicion_minima}, condicion_maxima: ${condicion_maxima}, valor_premio: ${valor_premio}, cantidad_ganadores: ${cantidad_ganadores}]`;
        }

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - condicion_minima: ${condicion_minima}`);
        console.log(`   - condicion_maxima: ${condicion_maxima}`);
        console.log(`   - valor_premio: ${valor_premio}`);
        console.log(`   - cantidad_ganadores: ${cantidad_ganadores}`);

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Premio válido`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores,
            estado,
            error: errorMsg.trim()
        });
    });

    await this.guardarResultadosPremios(codigopromocion, resultados);
}
async validarPremiosPromo19(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('premio_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de premios para la promoción ${codigopromocion}`);
        return;
    }

    // Premios válidos para promo 19
    const premiosEsperados = [
        { condicion_minima: 2, condicion_maxima: 0, valor_premio: 2500, cantidad_ganadores: 2 },
        { condicion_minima: 1, condicion_maxima: 0, valor_premio: 5000, cantidad_ganadores: 1 },
        { condicion_minima: 1, condicion_maxima: 0, valor_premio: 10000, cantidad_ganadores: 1 }
    ];

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_premio, cantidad_ganadores } = data;

        // Buscar el premio esperado según el intervalo y valor
        const esperado = premiosEsperados.find(p =>
            p.condicion_minima === condicion_minima &&
            p.condicion_maxima === condicion_maxima &&
            p.valor_premio === valor_premio
        );

        let estado = "OK";
        let errorMsg = "";

        if (!esperado) {
            estado = "ERROR";
            errorMsg += `Premio no esperado para el intervalo [${condicion_minima}, ${condicion_maxima}] y valor ${valor_premio}. `;
        } else if (cantidad_ganadores !== esperado.cantidad_ganadores) {
            estado = "ERROR";
            errorMsg += `Cantidad de ganadores incorrecta. Esperado: ${esperado.cantidad_ganadores}, Encontrado: ${cantidad_ganadores}. `;
        }

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Intervalo: [${condicion_minima}, ${condicion_maxima}]`);
        console.log(`   - Valor premio: ${valor_premio}`);
        console.log(`   - Cantidad ganadores: ${cantidad_ganadores}`);

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Premio válido`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores,
            estado,
            error: errorMsg.trim()
        });
    });

    await this.guardarResultadosPremios(codigopromocion, resultados);
}
async validarPremiosPromo22(codigopromocion: string) {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('premio_segmento_') && f.endsWith('.json'));

    if (files.length === 0) {
        console.error(`No se encontraron archivos de premios para la promoción ${codigopromocion}`);
        return;
    }

    // Premios válidos para promo 22
    const premiosEsperados = [
        { condicion_minima: 0, condicion_maxima: 0, valor_premio: 250, cantidad_ganadores: 2 },
        { condicion_minima: 0, condicion_maxima: 0, valor_premio: 500, cantidad_ganadores: 2 },
        { condicion_minima: 0, condicion_maxima: 0, valor_premio: 1000, cantidad_ganadores: 2 },
        { condicion_minima: 0, condicion_maxima: 0, valor_premio: 1999.99, cantidad_ganadores: 1 },
        { condicion_minima: 0, condicion_maxima: 0, valor_premio: 5000, cantidad_ganadores: 1 }
    ];

    const resultados: any[] = [];
    files.forEach(file => {
        const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
        const { condicion_minima, condicion_maxima, valor_premio, cantidad_ganadores } = data;

        // Buscar el premio esperado según el intervalo y valor
        const esperado = premiosEsperados.find(p =>
            p.condicion_minima === condicion_minima &&
            p.condicion_maxima === condicion_maxima &&
            p.valor_premio === valor_premio
        );

        let estado = "OK";
        let errorMsg = "";

        if (!esperado) {
            estado = "ERROR";
            errorMsg += `Premio no esperado para el intervalo [${condicion_minima}, ${condicion_maxima}] y valor ${valor_premio}. `;
        } else if (cantidad_ganadores !== esperado.cantidad_ganadores) {
            estado = "ERROR";
            errorMsg += `Cantidad de ganadores incorrecta. Esperado: ${esperado.cantidad_ganadores}, Encontrado: ${cantidad_ganadores}. `;
        }

        // Log de validación
        console.log(`\n🔎 Validando archivo: ${file}`);
        console.log(`   - Intervalo: [${condicion_minima}, ${condicion_maxima}]`);
        console.log(`   - Valor premio: ${valor_premio}`);
        console.log(`   - Cantidad ganadores: ${cantidad_ganadores}`);

        if (estado === "ERROR") {
            console.error(`❌ Archivo ${file}: ${errorMsg}`);
        } else {
            console.log(`✅ Archivo ${file}: Premio válido`);
        }

        resultados.push({
            archivo: file,
            condicion_minima,
            condicion_maxima,
            valor_premio,
            cantidad_ganadores,
            estado,
            error: errorMsg.trim()
        });
    });

    await this.guardarResultadosPremios(codigopromocion, resultados);
}
  private ajustarYFormatearFecha(date: Date): string {
     const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const min = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}`;
  }
  
async validarEtapasSegmento(codigopromocion: string) {
    switch (codigopromocion) {
        case "17":
            return this.validarEtapas17(codigopromocion);
        case "18":
            return this.validarEtapas18(codigopromocion);
        case "19":
            return this.validarEtapas18(codigopromocion);
        case "22":
            return this.validarEtapas22(codigopromocion);
        default:
            throw new Error("Promoción no soportada para validación de etapas");
    }
}

async validarEtapas17(codigopromocion: string): Promise<any[]> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('etapa_segmento_') && f.endsWith('.json'));

    const erroresTotales: any[] = [];
    const resultados: any[] = [];

    // Agrupar etapas por segmento
    const etapasPorSegmento: Record<string, any[]> = {};
    files.forEach(file => {
      const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
      const idSegmento = data.id_ejecucion_segmento;
      if (!etapasPorSegmento[idSegmento]) etapasPorSegmento[idSegmento] = [];
      etapasPorSegmento[idSegmento].push(data);
    });

    function truncSeconds(date: Date) {
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
        date.getMinutes(),
        date.getSeconds(),
        0
      );
    }

    for (const [idSegmento, etapas] of Object.entries(etapasPorSegmento)) {
      etapas.sort((a, b) => new Date(a.fecha_inicio).getTime() - new Date(b.fecha_inicio).getTime());

      let validaciones: any[] = [];
      const etapa = (nombre: string) => etapas.find(e => e.nombre_etapa === nombre);

      // PLANIFICADO
      {
        const planificado = etapa("PLANIFICADO");
        const acumulacion = etapa("ACUMULACIÓN");
        if (planificado && acumulacion) {
          const planIni = new Date(planificado.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffDias = (acumIni.getTime() - planIni.getTime()) / (1000 * 60 * 60 * 24);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Inicio un día antes de ACUMULACIÓN",
            estado: Math.round(diffDias) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 24 * 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planIni),
            mensaje: Math.round(diffDias) === 1 ? "Inicio correcto." : "PLANIFICADO no inicia un día antes de ACUMULACIÓN."
          });
          const planFin = new Date(planificado.fecha_fin);
          const diffHoras = (acumIni.getTime() - planFin.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Fin una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planFin),
            mensaje: Math.round(diffHoras) === 1 ? "Fin correcto." : "PLANIFICADO no termina una hora antes de ACUMULACIÓN."
          });
        }
      }

      // PRE EJECUCION
      {
        const preejecucion = etapa("PRE EJECUCION");
        const acumulacion = etapa("ACUMULACIÓN");
        if (preejecucion && acumulacion) {
          const preIni = new Date(preejecucion.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffHoras = (acumIni.getTime() - preIni.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PRE EJECUCION",
            regla: "Inicio una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(preIni),
            mensaje: Math.round(diffHoras) === 1 ? "Inicio correcto." : "PRE EJECUCION no inicia una hora antes de ACUMULACIÓN."
          });
        }
      }

      // VALIDACION
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const validacion = etapa("VALIDACION");
        if (acumulacion && validacion) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const valIni = truncSeconds(new Date(validacion.fecha_inicio));
          const valFin = truncSeconds(new Date(validacion.fecha_fin));
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Inicio justo después de ACUMULACIÓN",
            estado: acumFin.getTime() + 1000 === valIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(valIni),
            mensaje: acumFin.getTime() + 1000 === valIni.getTime() ? "Inicio correcto." : "VALIDACION no inicia justo después de ACUMULACIÓN."
          });
          const duracionMin = (valFin.getTime() - valIni.getTime()) / (1000 * 60);
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Duración 30 minutos",
            estado: Math.round(duracionMin) === 30 ? "OK" : "ERROR",
            valor_esperado: "30 minutos",
            valor_encontrado: `${duracionMin} minutos`,
            mensaje: Math.round(duracionMin) === 30 ? "Duración correcta." : "VALIDACION no dura 30 minutos."
          });
        }
      }

      // RECALCULO
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const recalculo = etapa("RECALCULO");
        if (acumulacion && recalculo) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const recIni = truncSeconds(new Date(recalculo.fecha_inicio));
          const recFin = truncSeconds(new Date(recalculo.fecha_fin));
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Inicio justo después de ACUMULACIÓN",
            estado: acumFin.getTime() + 1000 === recIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(recIni),
            mensaje: acumFin.getTime() + 1000 === recIni.getTime() ? "Inicio correcto." : "RECALCULO no inicia justo después de ACUMULACIÓN."
          });
          const duracionMin = (recFin.getTime() - recIni.getTime()) / (1000 * 60);
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Duración 30 minutos",
            estado: Math.round(duracionMin) === 30 ? "OK" : "ERROR",
            valor_esperado: "30 minutos",
            valor_encontrado: `${duracionMin} minutos`,
            mensaje: Math.round(duracionMin) === 30 ? "Duración correcta." : "RECALCULO no dura 30 minutos."
          });
        }
      }

      // RESULTADO
      {
        const recalculo = etapa("RECALCULO");
        const resultado = etapa("RESULTADO");
        if (recalculo && resultado) {
          const recFin = truncSeconds(new Date(recalculo.fecha_fin));
          const resIni = truncSeconds(new Date(resultado.fecha_inicio));
          validaciones.push({
            etapa: "RESULTADO",
            regla: "Inicio justo después de RECALULO",
            estado: recFin.getTime() + 1000 === resIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(recFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(resIni),
            mensaje: recFin.getTime() + 1000 === resIni.getTime() ? "Inicio correcto." : "RESULTADO no inicia justo después de RECALULO."
          });
        }
      }

      // RESULTADO IVIEW
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const resultadoIview = etapa("RESULTADO IVIEW");
        if (acumulacion && resultadoIview) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const esperadoInicio = new Date(acumFin.getTime() + 1000 + 30 * 60 * 1000);
          const iviewIni = truncSeconds(new Date(resultadoIview.fecha_inicio));
          validaciones.push({
            etapa: "RESULTADO IVIEW",
            regla: "Inicio 30 min después de ACUMULACIÓN",
            estado: Math.abs(iviewIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoInicio),
            valor_encontrado: this.ajustarYFormatearFecha(iviewIni),
            mensaje: Math.abs(iviewIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "Inicio correcto." : "RESULTADO IVIEW no inicia 30 min después de ACUMULACIÓN."
          });
          const iviewFin = new Date(resultadoIview.fecha_fin);
          const esperadoFin = new Date(iviewIni);
          esperadoFin.setDate(esperadoFin.getDate() + 1);
          esperadoFin.setHours(13, 59, 59, 0);
          validaciones.push({
            etapa: "RESULTADO IVIEW",
            regla: "Fin el día siguiente a las 13:59:59",
            estado: Math.abs(iviewFin.getTime() - esperadoFin.getTime()) <= 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoFin),
            valor_encontrado: this.ajustarYFormatearFecha(iviewFin),
            mensaje: Math.abs(iviewFin.getTime() - esperadoFin.getTime()) <= 1000 ? "Fin correcto." : "RESULTADO IVIEW no termina el día siguiente a las 13:59:59."
          });
        }
      }

      // PAGOS FISICO
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const pagosFisico = etapa("PAGOS FISICO");
        if (acumulacion && pagosFisico) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const esperadoInicio = new Date(acumFin.getTime() + 1000 + 4 * 60 * 60 * 1000);
          const pagosIni = truncSeconds(new Date(pagosFisico.fecha_inicio));
          validaciones.push({
            etapa: "PAGOS FISICO",
            regla: "Inicio 4 horas después de ACUMULACIÓN",
            estado: Math.abs(pagosIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoInicio),
            valor_encontrado: this.ajustarYFormatearFecha(pagosIni),
            mensaje: Math.abs(pagosIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "Inicio correcto." : "PAGOS FISICO no inicia 4 horas después de ACUMULACIÓN."
          });
          const pagosFin = new Date(pagosFisico.fecha_fin);
          const esperadoFin = new Date(pagosIni);
          esperadoFin.setDate(esperadoFin.getDate() + 1);
          esperadoFin.setHours(1, 30, 0, 0);
          validaciones.push({
            etapa: "PAGOS FISICO",
            regla: "Fin el día siguiente a la 01:30 am",
            estado: Math.abs(pagosFin.getTime() - esperadoFin.getTime()) <= 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoFin),
            valor_encontrado: this.ajustarYFormatearFecha(pagosFin),
            mensaje: Math.abs(pagosFin.getTime() - esperadoFin.getTime()) <= 1000 ? "Fin correcto." : "PAGOS FISICO no termina el día siguiente a la 01:30 am."
          });
        }
      }

      // PAGOS FISICO VENCIDOS
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const pagosFisicoVencidos = etapa("PAGOS FISICO VENCIDOS");
        if (acumulacion && pagosFisicoVencidos) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const esperadoInicio = new Date(acumFin);
          esperadoInicio.setDate(esperadoInicio.getDate() + 1);
          esperadoInicio.setHours(10, 0, 0, 0);
          const pagosVencIni = truncSeconds(new Date(pagosFisicoVencidos.fecha_inicio));
          validaciones.push({
            etapa: "PAGOS FISICO VENCIDOS",
            regla: "Inicio al día siguiente de ACUMULACIÓN a las 10:00",
            estado: Math.abs(pagosVencIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoInicio),
            valor_encontrado: this.ajustarYFormatearFecha(pagosVencIni),
            mensaje: Math.abs(pagosVencIni.getTime() - esperadoInicio.getTime()) <= 1000 ? "Inicio correcto." : "PAGOS FISICO VENCIDOS no inicia al día siguiente de ACUMULACIÓN a las 10:00."
          });
        }
      }

      // FINALIZADO
      {
        const resultado = etapa("RESULTADO");
        const finalizado = etapa("FINALIZADO");
        if (resultado && finalizado) {
          const resFin = truncSeconds(new Date(resultado.fecha_fin));
          const finIni = truncSeconds(new Date(finalizado.fecha_inicio));
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Inicio justo después de RESULTADO",
            estado: resFin.getTime() + 1000 === finIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(resFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(finIni),
            mensaje: resFin.getTime() + 1000 === finIni.getTime() ? "Inicio correcto." : "FINALIZADO no inicia justo después de RESULTADO."
          });
          const finFin = truncSeconds(new Date(finalizado.fecha_fin));
          const duracionMin = (finFin.getTime() - finIni.getTime()) / (1000 * 60);
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Duración 30 minutos",
            estado: Math.round(duracionMin) === 30 ? "OK" : "ERROR",
            valor_esperado: "30 minutos",
            valor_encontrado: `${duracionMin} minutos`,
            mensaje: Math.round(duracionMin) === 30 ? "Duración correcta." : "FINALIZADO no dura 30 minutos."
          });
        }
      }

      // Log para cada validación
      validaciones.forEach(v => {
        if (v.estado === "OK") {
          console.log(`[OK] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        } else {
          console.error(`[ERROR] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        }
      });

      resultados.push({
        segmento: idSegmento,
        validaciones
      });

      erroresTotales.push(...validaciones.filter(v => v.estado === "ERROR"));
    }

    const outFolder = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
    if (!fs.existsSync(outFolder)) fs.mkdirSync(outFolder, { recursive: true });
    const outFile = path.join(outFolder, 'validacion_etapas.json');
    fs.writeFileSync(outFile, JSON.stringify(resultados, null, 2), 'utf-8');

    return erroresTotales;
}

async validarEtapas18(codigopromocion: string): Promise<any[]> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('etapa_segmento_') && f.endsWith('.json'));

    const erroresTotales: any[] = [];
    const resultados: any[] = [];

    // Agrupar etapas por segmento
    const etapasPorSegmento: Record<string, any[]> = {};
    files.forEach(file => {
      const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
      const idSegmento = data.id_ejecucion_segmento;
      if (!etapasPorSegmento[idSegmento]) etapasPorSegmento[idSegmento] = [];
      etapasPorSegmento[idSegmento].push(data);
    });

    function truncSeconds(date: Date) {
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
        date.getMinutes(),
        date.getSeconds(),
        0
      );
    }

    for (const [idSegmento, etapas] of Object.entries(etapasPorSegmento)) {
      etapas.sort((a, b) => new Date(a.fecha_inicio).getTime() - new Date(b.fecha_inicio).getTime());

      let validaciones: any[] = [];
      const etapa = (nombre: string) => etapas.find(e => e.nombre_etapa === nombre);

      // PLANIFICADO
      {
        const planificado = etapa("PLANIFICADO");
        const acumulacion = etapa("ACUMULACIÓN");
        if (planificado && acumulacion) {
          const planIni = new Date(planificado.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffDias = (acumIni.getTime() - planIni.getTime()) / (1000 * 60 * 60 * 24);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Inicio un día antes de ACUMULACIÓN",
            estado: Math.round(diffDias) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 24 * 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planIni),
            mensaje: Math.round(diffDias) === 1 ? "Inicio correcto." : "PLANIFICADO no inicia un día antes de ACUMULACIÓN."
          });
          const planFin = new Date(planificado.fecha_fin);
          const diffHoras = (acumIni.getTime() - planFin.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Fin una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planFin),
            mensaje: Math.round(diffHoras) === 1 ? "Fin correcto." : "PLANIFICADO no termina una hora antes de ACUMULACIÓN."
          });
        }
      }

      // PRE EJECUCION
      {
        const preejecucion = etapa("PRE EJECUCION");
        const acumulacion = etapa("ACUMULACIÓN");
        if (preejecucion && acumulacion) {
          const preIni = new Date(preejecucion.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffHoras = (acumIni.getTime() - preIni.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PRE EJECUCION",
            regla: "Inicio una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(preIni),
            mensaje: Math.round(diffHoras) === 1 ? "Inicio correcto." : "PRE EJECUCION no inicia una hora antes de ACUMULACIÓN."
          });
        }
      }

      // ACUMULACIÓN
      {
        const acumulacion = etapa("ACUMULACIÓN");
        if (acumulacion) {
          const acumIni = new Date(acumulacion.fecha_inicio);
          validaciones.push({
            etapa: "ACUMULACIÓN",
            regla: "Inicio a las 00:00 horas",
            estado: acumIni.getHours() === 0 && acumIni.getMinutes() === 0 && acumIni.getSeconds() === 0 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.setHours(0,0,0,0))),
            valor_encontrado: this.ajustarYFormatearFecha(acumIni),
            mensaje: acumIni.getHours() === 0 && acumIni.getMinutes() === 0 && acumIni.getSeconds() === 0 ? "Inicio correcto." : "ACUMULACIÓN no inicia a las 00:00 horas."
          });
        }
      }

      // RECALCULO
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const recalculo = etapa("RECALCULO");
        if (acumulacion && recalculo) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const recIni = truncSeconds(new Date(recalculo.fecha_inicio));
          const recFin = truncSeconds(new Date(recalculo.fecha_fin));
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Inicio justo después de ACUMULACIÓN",
            estado: acumFin.getTime() + 1000 === recIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(recIni),
            mensaje: acumFin.getTime() + 1000 === recIni.getTime() ? "Inicio correcto." : "RECALCULO no inicia justo después de ACUMULACIÓN."
          });
          const duracionMin = (recFin.getTime() - recIni.getTime()) / (1000 * 60);
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Duración 30 minutos",
            estado: Math.round(duracionMin) === 30 ? "OK" : "ERROR",
            valor_esperado: "30 minutos",
            valor_encontrado: `${duracionMin} minutos`,
            mensaje: Math.round(duracionMin) === 30 ? "Duración correcta." : "RECALCULO no dura 30 minutos."
          });
        }
      }

      // CANJES
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const canjes = etapa("CANJES");
        const sorteo = etapa("SORTEO");
        if (acumulacion && canjes && sorteo) {
          const acumFin = new Date(acumulacion.fecha_fin);
          const canjesIni = new Date(canjes.fecha_inicio);
          const canjesFin = new Date(canjes.fecha_fin);
          const sorteoIni = new Date(sorteo.fecha_inicio);

          // Inicia el último día de acumulación a las 00:00
          const esperadoCanjesIni = new Date(acumFin);
          esperadoCanjesIni.setHours(0,0,0,0);

          validaciones.push({
            etapa: "CANJES",
            regla: "Inicio el último día de acumulación a las 00:00",
            estado: canjesIni.getTime() === esperadoCanjesIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoCanjesIni),
            valor_encontrado: this.ajustarYFormatearFecha(canjesIni),
            mensaje: canjesIni.getTime() === esperadoCanjesIni.getTime() ? "Inicio correcto." : "CANJES no inicia el último día de acumulación a las 00:00."
          });

          // Termina un segundo antes del sorteo
          validaciones.push({
            etapa: "CANJES",
            regla: "Fin un segundo antes del sorteo",
            estado: canjesFin.getTime() + 1000 === sorteoIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(sorteoIni.getTime() - 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(canjesFin),
            mensaje: canjesFin.getTime() + 1000 === sorteoIni.getTime() ? "Fin correcto." : "CANJES no termina un segundo antes del sorteo."
          });
        }
      }

      // SORTEO
      {
        const sorteo = etapa("SORTEO");
        const canjes = etapa("CANJES");
        if (sorteo && canjes) {
          const sorteoIni = new Date(sorteo.fecha_inicio);
          const canjesFin = new Date(canjes.fecha_fin);

          // No puede cruzarse con canjes (excepto salta y gana, aquí no se valida esa excepción)
          validaciones.push({
            etapa: "SORTEO",
            regla: "No cruzarse con CANJES",
            estado: sorteoIni.getTime() === canjesFin.getTime() + 1000 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(canjesFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(sorteoIni),
            mensaje: sorteoIni.getTime() === canjesFin.getTime() + 1000 ? "No se cruza con CANJES." : "SORTEO se cruza con CANJES."
          });
        }
      }

            // VALIDACION
      {
        const canjes = etapa("CANJES");
        const sorteo = etapa("SORTEO");
        const validacion = etapa("VALIDACION");
        if (canjes && sorteo && validacion) {
          const canjesFin = truncSeconds(new Date(canjes.fecha_fin));
          const valIni = truncSeconds(new Date(validacion.fecha_inicio));
          const valFin = truncSeconds(new Date(validacion.fecha_fin));
          const sorteoIni = truncSeconds(new Date(sorteo.fecha_inicio));
          const sorteoFin = truncSeconds(new Date(sorteo.fecha_fin));

          // Inicia al segundo siguiente del fin de CANJES
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Inicio justo después de CANJES",
            estado: canjesFin.getTime() + 1000 === valIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(canjesFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(valIni),
            mensaje: canjesFin.getTime() + 1000 === valIni.getTime() ? "Inicio correcto." : "VALIDACION no inicia justo después de CANJES."
          });

          // Duración igual a la de SORTEO
          const duracionSorteo = sorteoFin.getTime() - sorteoIni.getTime();
          const duracionValidacion = valFin.getTime() - valIni.getTime();
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Duración igual a SORTEO",
            estado: duracionSorteo === duracionValidacion ? "OK" : "ERROR",
            valor_esperado: `${duracionSorteo / 1000} segundos`,
            valor_encontrado: `${duracionValidacion / 1000} segundos`,
            mensaje: duracionSorteo === duracionValidacion ? "Duración correcta." : "VALIDACION no dura igual que SORTEO."
          });
        }
      }

      // RESULTADO
      {
          // Encuentra la última VALIDACION
        const validacionesEtapas = etapas.filter(e => e.nombre_etapa === "VALIDACION");
        const ultimaValidacion = validacionesEtapas.length > 0
          ? validacionesEtapas.reduce((a, b) =>
              new Date(a.fecha_fin) > new Date(b.fecha_fin) ? a : b
            )
          : undefined;

        // Encuentra la etapa RESULTADO
        const resultado = etapa("RESULTADO");

        if (ultimaValidacion && resultado) {
          const valFin = truncSeconds(new Date(ultimaValidacion.fecha_fin));
          const resIni = truncSeconds(new Date(resultado.fecha_inicio));
          validaciones.push({
            etapa: "RESULTADO",
            regla: "Inicio justo después de VALIDACION",
            estado: valFin.getTime() + 1000 === resIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(valFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(resIni),
            mensaje: valFin.getTime() + 1000 === resIni.getTime() ? "Inicio correcto." : "RESULTADO no inicia justo después de VALIDACION."
          });
        }
      }

      // FINALIZADO
      {
        const resultado = etapa("RESULTADO");
        const finalizado = etapa("FINALIZADO");
        if (resultado && finalizado) {
          const resFin = truncSeconds(new Date(resultado.fecha_fin));
          const finIni = truncSeconds(new Date(finalizado.fecha_inicio));
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Inicio justo después de RESULTADO",
            estado: resFin.getTime() + 1000 === finIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(resFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(finIni),
            mensaje: resFin.getTime() + 1000 === finIni.getTime() ? "Inicio correcto." : "FINALIZADO no inicia justo después de RESULTADO."
          });
          // Duración de 3 horas hasta las 5:59:59
          const finFin = new Date(finalizado.fecha_fin);
          const esperadoFin = new Date(finIni);
          esperadoFin.setHours(5, 59, 59, 0);
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Duración 3 horas hasta las 5:59:59",
            estado: finFin.getHours() === 5 && finFin.getMinutes() === 59 && finFin.getSeconds() === 59 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoFin),
            valor_encontrado: this.ajustarYFormatearFecha(finFin),
            mensaje: finFin.getHours() === 5 && finFin.getMinutes() === 59 && finFin.getSeconds() === 59 ? "Duración correcta." : "FINALIZADO no termina a las 5:59:59."
          });
        }
      }

      validaciones.forEach(v => {
        if (v.estado === "OK") {
          console.log(`[OK] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        } else {
          console.error(`[ERROR] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        }
      });

      resultados.push({
        segmento: idSegmento,
        validaciones
      });

      erroresTotales.push(...validaciones.filter(v => v.estado === "ERROR"));
    }

    const outFolder = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
    if (!fs.existsSync(outFolder)) fs.mkdirSync(outFolder, { recursive: true });
    const outFile = path.join(outFolder, 'validacion_etapas.json');
    fs.writeFileSync(outFile, JSON.stringify(resultados, null, 2), 'utf-8');

    return erroresTotales;
}


async validarEtapas22(codigopromocion: string): Promise<any[]> {
    const folderPath = path.join('pages', 'Brief', 'DatosBD', codigopromocion);
    const files = fs.readdirSync(folderPath).filter(f => f.startsWith('etapa_segmento_') && f.endsWith('.json'));

    const erroresTotales: any[] = [];
    const resultados: any[] = [];

    // Agrupar etapas por segmento
    const etapasPorSegmento: Record<string, any[]> = {};
    files.forEach(file => {
      const data = JSON.parse(fs.readFileSync(path.join(folderPath, file), 'utf-8'));
      const idSegmento = data.id_ejecucion_segmento;
      if (!etapasPorSegmento[idSegmento]) etapasPorSegmento[idSegmento] = [];
      etapasPorSegmento[idSegmento].push(data);
    });

    function truncSeconds(date: Date) {
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        date.getHours(),
        date.getMinutes(),
        date.getSeconds(),
        0
      );
    }

    for (const [idSegmento, etapas] of Object.entries(etapasPorSegmento)) {
      etapas.sort((a, b) => new Date(a.fecha_inicio).getTime() - new Date(b.fecha_inicio).getTime());

      let validaciones: any[] = [];
      const etapa = (nombre: string) => etapas.find(e => e.nombre_etapa === nombre);

      // PLANIFICADO
      {
        const planificado = etapa("PLANIFICADO");
        const acumulacion = etapa("ACUMULACIÓN");
        if (planificado && acumulacion) {
          const planIni = new Date(planificado.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffDias = (acumIni.getTime() - planIni.getTime()) / (1000 * 60 * 60 * 24);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Inicio un día antes de ACUMULACIÓN",
            estado: Math.round(diffDias) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 24 * 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planIni),
            mensaje: Math.round(diffDias) === 1 ? "Inicio correcto." : "PLANIFICADO no inicia un día antes de ACUMULACIÓN."
          });
          const planFin = new Date(planificado.fecha_fin);
          const diffHoras = (acumIni.getTime() - planFin.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PLANIFICADO",
            regla: "Fin una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(planFin),
            mensaje: Math.round(diffHoras) === 1 ? "Fin correcto." : "PLANIFICADO no termina una hora antes de ACUMULACIÓN."
          });
        }
      }

      // PRE EJECUCION
      {
        const preejecucion = etapa("PRE EJECUCION");
        const acumulacion = etapa("ACUMULACIÓN");
        if (preejecucion && acumulacion) {
          const preIni = new Date(preejecucion.fecha_inicio);
          const acumIni = new Date(acumulacion.fecha_inicio);
          const diffHoras = (acumIni.getTime() - preIni.getTime()) / (1000 * 60 * 60);
          validaciones.push({
            etapa: "PRE EJECUCION",
            regla: "Inicio una hora antes de ACUMULACIÓN",
            estado: Math.round(diffHoras) === 1 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.getTime() - 60 * 60 * 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(preIni),
            mensaje: Math.round(diffHoras) === 1 ? "Inicio correcto." : "PRE EJECUCION no inicia una hora antes de ACUMULACIÓN."
          });
        }
      }

      // ACUMULACIÓN
      {
        const acumulacion = etapa("ACUMULACIÓN");
        if (acumulacion) {
          const acumIni = new Date(acumulacion.fecha_inicio);
          validaciones.push({
            etapa: "ACUMULACIÓN",
            regla: "Inicio a las 00:00 horas",
            estado: acumIni.getHours() === 0 && acumIni.getMinutes() === 0 && acumIni.getSeconds() === 0 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumIni.setHours(0,0,0,0))),
            valor_encontrado: this.ajustarYFormatearFecha(acumIni),
            mensaje: acumIni.getHours() === 0 && acumIni.getMinutes() === 0 && acumIni.getSeconds() === 0 ? "Inicio correcto." : "ACUMULACIÓN no inicia a las 00:00 horas."
          });
        }
      }

      // RECALCULO
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const recalculo = etapa("RECALCULO");
        if (acumulacion && recalculo) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const recIni = truncSeconds(new Date(recalculo.fecha_inicio));
          const recFin = truncSeconds(new Date(recalculo.fecha_fin));
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Inicio justo después de ACUMULACIÓN",
            estado: acumFin.getTime() + 1000 === recIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(recIni),
            mensaje: acumFin.getTime() + 1000 === recIni.getTime() ? "Inicio correcto." : "RECALCULO no inicia justo después de ACUMULACIÓN."
          });
          const duracionMin = (recFin.getTime() - recIni.getTime()) / (1000 * 60);
          validaciones.push({
            etapa: "RECALCULO",
            regla: "Duración 30 minutos",
            estado: Math.round(duracionMin) === 30 ? "OK" : "ERROR",
            valor_esperado: "30 minutos",
            valor_encontrado: `${duracionMin} minutos`,
            mensaje: Math.round(duracionMin) === 30 ? "Duración correcta." : "RECALCULO no dura 30 minutos."
          });
        }
      }

      // CANJES (Salta y Gana: inicia al segundo siguiente de ACUMULACIÓN y termina al día siguiente a las 01:09:59)
      {
        const acumulacion = etapa("ACUMULACIÓN");
        const canjes = etapa("CANJES");
        if (acumulacion && canjes) {
          const acumFin = truncSeconds(new Date(acumulacion.fecha_fin));
          const canjesIni = truncSeconds(new Date(canjes.fecha_inicio));
          const canjesFin = truncSeconds(new Date(canjes.fecha_fin));

          // Inicio: al segundo siguiente del fin de ACUMULACIÓN
          validaciones.push({
            etapa: "CANJES",
            regla: "Inicio justo después de ACUMULACIÓN",
            estado: acumFin.getTime() + 1000 === canjesIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(acumFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(canjesIni),
            mensaje: acumFin.getTime() + 1000 === canjesIni.getTime() ? "Inicio correcto." : "CANJES no inicia justo después de ACUMULACIÓN."
          });

          // Fin: al día siguiente a las 01:09:59
          const esperadoFin = new Date(canjesIni);
          esperadoFin.setDate(esperadoFin.getDate() + 1);
          esperadoFin.setHours(1, 9, 59, 0);
          validaciones.push({
            etapa: "CANJES",
            regla: "Fin al día siguiente a las 01:09:59",
            estado: canjesFin.getTime() === esperadoFin.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoFin),
            valor_encontrado: this.ajustarYFormatearFecha(canjesFin),
            mensaje: canjesFin.getTime() === esperadoFin.getTime() ? "Fin correcto." : "CANJES no termina al día siguiente a las 01:09:59."
          });
        }
      }

      // SORTEO (excepción: puede cruzarse con CANJES)
      {
        // No se valida cruce con CANJES por excepción Salta y Gana
      }

      // VALIDACION (igual que promo 18, pero con regla actualizada)
      {
        const canjes = etapa("CANJES");
        const sorteo = etapa("SORTEO");
        const validacion = etapa("VALIDACION");
        if (canjes && sorteo && validacion) {
          const canjesFin = truncSeconds(new Date(canjes.fecha_fin));
          const valIni = truncSeconds(new Date(validacion.fecha_inicio));
          const valFin = truncSeconds(new Date(validacion.fecha_fin));
          const sorteoIni = truncSeconds(new Date(sorteo.fecha_inicio));
          const sorteoFin = truncSeconds(new Date(sorteo.fecha_fin));

          // Inicia al segundo siguiente del fin de CANJES
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Inicio justo después de CANJES",
            estado: canjesFin.getTime() + 1000 === valIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(canjesFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(valIni),
            mensaje: canjesFin.getTime() + 1000 === valIni.getTime() ? "Inicio correcto." : "VALIDACION no inicia justo después de CANJES."
          });

          // Duración igual a la de SORTEO
          const duracionSorteo = sorteoFin.getTime() - sorteoIni.getTime();
          const duracionValidacion = valFin.getTime() - valIni.getTime();
          validaciones.push({
            etapa: "VALIDACION",
            regla: "Duración igual a SORTEO",
            estado: duracionSorteo === duracionValidacion ? "OK" : "ERROR",
            valor_esperado: `${duracionSorteo / 1000} segundos`,
            valor_encontrado: `${duracionValidacion / 1000} segundos`,
            mensaje: duracionSorteo === duracionValidacion ? "Duración correcta." : "VALIDACION no dura igual que SORTEO."
          });
        }
      }

      // RESULTADO
      {
        const validacion = etapa("VALIDACION");
        const resultado = etapa("RESULTADO");
        if (validacion && resultado) {
          const valFin = truncSeconds(new Date(validacion.fecha_fin));
          const resIni = truncSeconds(new Date(resultado.fecha_inicio));
          validaciones.push({
            etapa: "RESULTADO",
            regla: "Inicio justo después de VALIDACION",
            estado: valFin.getTime() + 1000 === resIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(valFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(resIni),
            mensaje: valFin.getTime() + 1000 === resIni.getTime() ? "Inicio correcto." : "RESULTADO no inicia justo después de VALIDACION."
          });
        }
      }

      // FINALIZADO
      {
        const resultado = etapa("RESULTADO");
        const finalizado = etapa("FINALIZADO");
        if (resultado && finalizado) {
          const resFin = truncSeconds(new Date(resultado.fecha_fin));
          const finIni = truncSeconds(new Date(finalizado.fecha_inicio));
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Inicio justo después de RESULTADO",
            estado: resFin.getTime() + 1000 === finIni.getTime() ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(new Date(resFin.getTime() + 1000)),
            valor_encontrado: this.ajustarYFormatearFecha(finIni),
            mensaje: resFin.getTime() + 1000 === finIni.getTime() ? "Inicio correcto." : "FINALIZADO no inicia justo después de RESULTADO."
          });
          // Duración de 3 horas hasta las 5:59:59
          const finFin = new Date(finalizado.fecha_fin);
          const esperadoFin = new Date(finIni);
          esperadoFin.setHours(5, 59, 59, 0);
          validaciones.push({
            etapa: "FINALIZADO",
            regla: "Duración 3 horas hasta las 5:59:59",
            estado: finFin.getHours() === 5 && finFin.getMinutes() === 59 && finFin.getSeconds() === 59 ? "OK" : "ERROR",
            valor_esperado: this.ajustarYFormatearFecha(esperadoFin),
            valor_encontrado: this.ajustarYFormatearFecha(finFin),
            mensaje: finFin.getHours() === 5 && finFin.getMinutes() === 59 && finFin.getSeconds() === 59 ? "Duración correcta." : "FINALIZADO no termina a las 5:59:59."
          });
        }
      }

      validaciones.forEach(v => {
        if (v.estado === "OK") {
          console.log(`[OK] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        } else {
          console.error(`[ERROR] Segmento ${idSegmento} - ${v.etapa} (${v.regla}): ${v.mensaje} | Esperado: ${v.valor_esperado} | Encontrado: ${v.valor_encontrado}`);
        }
      });

      resultados.push({
        segmento: idSegmento,
        validaciones
      });

      erroresTotales.push(...validaciones.filter(v => v.estado === "ERROR"));
    }

    const outFolder = path.join('pages', 'Brief', 'Validaciones', codigopromocion);
    if (!fs.existsSync(outFolder)) fs.mkdirSync(outFolder, { recursive: true });
    const outFile = path.join(outFolder, 'validacion_etapas.json');
    fs.writeFileSync(outFile, JSON.stringify(resultados, null, 2), 'utf-8');

    return erroresTotales;
}
}


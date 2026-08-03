// supabase.js - Auth + Postgres multi-cliente (réplica de firebase.js)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// TODO: pegar acá los valores de Project Settings → API
const SUPABASE_URL = "https://rwtmixsualqclbeutuhq.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_0TEs33sCLMsA2vPssnJKUQ_OfxmpSdJ";

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// =========================================================
// Autenticación
// =========================================================

export function onAuthChange(callback) {
    // Dispara al instante con la sesión actual (o null), y de nuevo en cada
    // login/logout — mismo comportamiento que onAuthStateChanged de Firebase.
    supabase.auth.getSession().then(({ data }) => {
        callback(data.session?.user ?? null);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) => {
        callback(session?.user ?? null);
    });
    return () => sub.subscription.unsubscribe(); // misma forma "unsubscribe" que Firebase
}

export async function loginUsuario(email, password) {
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) throw { code: "auth/invalid-credential", message: error.message };
    await asegurarUsuarioDoc(data.user);
    return data.user;
}

// -------------------------------------------------------
// Login por nombre de usuario (en vez de email)
// -------------------------------------------------------
// Igual que en Firebase, Supabase Auth solo entiende email+password. Acá el
// lookup usuario→email se hace vía la función RPC get_email_by_usuario
// (ver schema.sql), que es SECURITY DEFINER: devuelve UN email si el
// username existe, sin exponer el resto de la tabla "usernames" (a
// diferencia de Firestore, una policy de SELECT en Postgres expone la tabla
// entera, por eso acá no hay policy de lectura directa, solo la función).
export async function loginConUsuario(usuario, password) {
    const { data: email, error } = await supabase.rpc("get_email_by_usuario", {
        p_usuario: usuario
    });
    if (error || !email) {
        throw { code: "auth/invalid-credential" };
    }
    return loginUsuario(email, password);
}

export async function obtenerNombreUsuario(uid) {
    const { data, error } = await supabase
        .from("usuarios")
        .select("nombre")
        .eq("id", uid)
        .maybeSingle();
    if (error) return null;
    return data?.nombre ?? null;
}

export function logoutUsuario() {
    return supabase.auth.signOut();
}

async function asegurarUsuarioDoc(user) {
    const { data } = await supabase
        .from("usuarios")
        .select("id")
        .eq("id", user.id)
        .maybeSingle();
    if (!data) {
        await supabase.from("usuarios").insert({
            id: user.id,
            email: user.email
        });
    }
}

// =========================================================
// Catálogo de productos (tabla "productos", 1 fila por producto por cliente)
// =========================================================
// Nota: a diferencia de sanitizarCodigo() en Firebase (necesario porque
// Firestore no permite ciertos caracteres en un ID de documento), acá no
// hace falta: el código va tal cual en una columna de texto normal.

export async function obtenerCatalogo(uid) {
    // Supabase (PostgREST) devuelve máximo 1000 filas por consulta si no se
    // pagina explícitamente, sin importar cuántas haya realmente en la
    // tabla. Acá se pide en páginas de 1000 hasta que una página vuelva con
    // menos de 1000 filas (señal de que ya no queda nada más por traer).
    const PAGINA = 1000;
    let desde = 0;
    let todos = [];
    while (true) {
        const { data, error } = await supabase
            .from("productos")
            .select("*")
            .eq("usuario", uid)
            .range(desde, desde + PAGINA - 1);
        if (error) throw error;
        todos = todos.concat(data);
        if (data.length < PAGINA) break;
        desde += PAGINA;
    }
    return todos.map(mapProductoDesdeDB);
}

// Escucha el catálogo EN TIEMPO REAL vía Supabase Realtime (Postgres
// Replication). callback(productos) se llama con la lista completa cada vez
// que hay un cambio — igual que escucharCatalogo() con onSnapshot.
//
// Diferencia importante con Firestore: acá no llega automáticamente "la
// lista completa ya actualizada", solo el evento de qué fila cambió. Por
// eso mantenemos una copia local (cache) y la vamos parchando fila por fila.
//
// Devuelve una función "unsubscribe", igual que la versión de Firebase.
export function escucharCatalogo(uid, callback, onError) {
    let cache = [];
    let timerError = null;

    obtenerCatalogo(uid)
        .then(productos => {
            cache = productos;
            callback(cache);
        })
        .catch(err => {
            console.error("❌ Error cargando catálogo inicial:", err);
            if (onError) onError(err);
        });

    const canal = supabase
        .channel(`productos-${uid}`)
        .on("postgres_changes",
            { event: "*", schema: "public", table: "productos", filter: `usuario=eq.${uid}` },
            (payload) => {
                const codigo = (payload.new?.codigo) ?? (payload.old?.codigo);
                cache = cache.filter(p => p.codigo !== codigo);
                if (payload.eventType !== "DELETE") {
                    cache.push(mapProductoDesdeDB(payload.new));
                }
                callback(cache);
            }
        )
        .subscribe((status, err) => {
            if (status === "SUBSCRIBED") {
                if (timerError) { clearTimeout(timerError); timerError = null; }
                return;
            }
            // Al conectar (sobre todo justo después del login, o al volver de
            // que el navegador durmiera la pestaña) el canal puede pasar un
            // instante por CHANNEL_ERROR/TIMED_OUT antes de reconectarse solo
            // y terminar en SUBSCRIBED. Si avisáramos ante el primer error,
            // el toast aparecería en cada carga aunque todo termine andando
            // bien. Por eso esperamos un toque a ver si se resuelve solo
            // antes de mostrar el aviso.
            if ((status === "CHANNEL_ERROR" || status === "TIMED_OUT") && !timerError) {
                timerError = setTimeout(() => {
                    timerError = null;
                    if (onError) onError(err ?? new Error(status));
                }, 4000);
            }
        });

    return () => supabase.removeChannel(canal);
}

function mapProductoDesdeDB(row) {
    // Mismo "shape" que usaba app.js con los documentos de Firestore
    return {
        usuario: row.usuario,
        codigo: row.codigo,
        descripcion: row.descripcion,
        unidades: row.unidades,
        stock: row.stock,
        actualizado: row.actualizado
    };
}

function mapProductoParaDB(uid, p) {
    return {
        usuario: uid,
        codigo: p.codigoArt,
        descripcion: p.articulo,
        unidades: p.unidades,
        stock: p.stock_unidad,
        actualizado: new Date().toISOString()
    };
}

// app.js fue escrito para Firestore, donde las fechas llegan como objetos
// Timestamp con método .toDate(). Postgres/Supabase devuelve fechas como
// texto ISO plano, que no tiene ese método. En vez de reescribir cada lugar
// de app.js que llama a .toDate(), envolvemos acá el string en un objeto
// mínimo que se comporta igual: así app.js no necesita cambios.
function comoTimestamp(fechaISO) {
    if (!fechaISO) return null;
    return { toDate: () => new Date(fechaISO) };
}

// Carga inicial (o reemplazo) del catálogo desde el .txt. Postgres no tiene
// el límite de 500 escrituras por batch de Firestore, pero igual mandamos
// en lotes para no pasarnos del tamaño de request.
//
// IMPORTANTE: a diferencia de Firestore (donde un batch.set() repetido sobre
// el mismo ID de documento dentro del mismo batch simplemente se pisa, "gana
// el último"), Postgres tira error si un mismo upsert intenta tocar la
// misma fila (mismo usuario+codigo) dos veces en la MISMA sentencia. Un .txt
// real de stock puede traer códigos repetidos (variantes, renglones
// duplicados del sistema origen), así que deduplicamos cada lote antes de
// mandarlo, quedándonos con la última ocurrencia — mismo criterio que tenías
// en Firebase.
function dedupePorCodigo(productos) {
    const porCodigo = new Map();
    for (const p of productos) {
        porCodigo.set(p.codigoArt, p); // la última ocurrencia pisa a la anterior
    }
    return [...porCodigo.values()];
}

export async function importarCatalogo(uid, productos) {
    const CHUNK = 450;
    const sinDuplicados = dedupePorCodigo(productos);
    for (let i = 0; i < sinDuplicados.length; i += CHUNK) {
        const lote = sinDuplicados.slice(i, i + CHUNK).map(p => mapProductoParaDB(uid, p));
        const { error } = await supabase
            .from("productos")
            .upsert(lote, { onConflict: "usuario,codigo" });
        if (error) throw error;
    }
}

export async function crearProducto(uid, producto) {
    const { error } = await supabase
        .from("productos")
        .upsert(mapProductoParaDB(uid, producto), { onConflict: "usuario,codigo" });
    if (error) throw error;
}

export async function eliminarProducto(uid, codigoArt) {
    const { error } = await supabase
        .from("productos")
        .delete()
        .eq("usuario", uid)
        .eq("codigo", codigoArt);
    if (error) throw error;
}

export async function actualizarStockProducto(uid, producto) {
    try {
        const { error } = await supabase
            .from("productos")
            .upsert(mapProductoParaDB(uid, producto), { onConflict: "usuario,codigo" });
        if (error) throw error;
        return true;
    } catch (e) {
        console.error("❌ Error actualizando stock del producto:", e);
        return false;
    }
}

export async function borrarCatalogoCompleto(uid) {
    const { count, error } = await supabase
        .from("productos")
        .delete({ count: "exact" })
        .eq("usuario", uid);
    if (error) throw error;
    return count ?? 0;
}

// =========================================================
// Inventarios
// =========================================================

function nombreConteo() {
    const ahora = new Date();
    return `Conteo ${ahora.toLocaleDateString('es-AR')} ${ahora.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit', hour12: false })}`;
}

export async function asegurarInventarioActual(uid) {
    const { data } = await supabase
        .from("inventario_actual")
        .select("usuario")
        .eq("usuario", uid)
        .maybeSingle();
    if (!data) {
        const { error } = await supabase.from("inventario_actual").insert({
            usuario: uid,
            estado: "cerrado",
            nombre: null,
            fecha: null,
            fecha_cierre: null,
            items: {}
        });
        if (error) throw error;
    }
}

export async function abrirInventario(uid) {
    const { error } = await supabase
        .from("inventario_actual")
        .upsert({
            usuario: uid,
            estado: "abierto",
            nombre: nombreConteo(),
            fecha: new Date().toISOString(),
            fecha_cierre: null,
            items: {}
        }, { onConflict: "usuario" });
    if (error) throw error;
}

// callback(null) si el registro todavía no existe (igual que la versión Firebase).
export function escucharInventarioActual(uid, callback, onError) {
    let timerError = null;

    supabase
        .from("inventario_actual")
        .select("*")
        .eq("usuario", uid)
        .maybeSingle()
        .then(({ data }) => callback(data ? mapInventarioDesdeDB(data) : null));

    const canal = supabase
        .channel(`inventario-actual-${uid}`)
        .on("postgres_changes",
            { event: "*", schema: "public", table: "inventario_actual", filter: `usuario=eq.${uid}` },
            (payload) => {
                callback(payload.new ? mapInventarioDesdeDB(payload.new) : null);
            }
        )
        .subscribe((status, err) => {
            if (status === "SUBSCRIBED") {
                if (timerError) { clearTimeout(timerError); timerError = null; }
                return;
            }
            // Mismo motivo que en escucharCatalogo: un CHANNEL_ERROR/TIMED_OUT
            // inicial suele resolverse solo en un instante, así que esperamos
            // antes de avisar en vez de mostrar el toast ante el primer blip.
            if ((status === "CHANNEL_ERROR" || status === "TIMED_OUT") && !timerError) {
                timerError = setTimeout(() => {
                    timerError = null;
                    if (onError) onError(err ?? new Error(status));
                }, 4000);
            }
        });

    return () => supabase.removeChannel(canal);
}

function mapInventarioDesdeDB(row) {
    return {
        id: row.usuario, // mismo rol que el "uid_actual" de Firestore: identifica el doc actual
        estado: row.estado,
        nombre: row.nombre,
        fecha: comoTimestamp(row.fecha),
        fechaCierre: comoTimestamp(row.fecha_cierre),
        items: row.items || {}
    };
}

// Cierre atómico vía la función RPC cerrar_inventario (ver schema.sql).
// Devuelve true si se archivó algo en el Historial, igual que antes.
export async function cerrarInventario(uid) {
    const { data, error } = await supabase.rpc("cerrar_inventario", { p_usuario: uid });
    if (error) throw new Error(error.message);
    return data;
}

export async function eliminarInventario(inventarioId) {
    try {
        const { error } = await supabase
            .from("inventarios_historial")
            .delete()
            .eq("id", inventarioId);
        if (error) throw error;
        return true;
    } catch (e) {
        console.error("❌ Error eliminando inventario del historial:", e);
        return false;
    }
}

export async function obtenerInventariosCerrados(uid, desde = null, hasta = null) {
    let q = supabase
        .from("inventarios_historial")
        .select("*")
        .eq("usuario", uid)
        .order("fecha_cierre", { ascending: false })
        .limit(100);

    if (desde) q = q.gte("fecha_cierre", desde.toISOString());
    if (hasta) q = q.lte("fecha_cierre", hasta.toISOString());

    const { data, error } = await q;
    if (error) throw error;
    return data.map(row => ({
        id: row.id,
        usuario: row.usuario,
        estado: row.estado,
        nombre: row.nombre,
        fecha: comoTimestamp(row.fecha),
        fechaCierre: comoTimestamp(row.fecha_cierre),
        items: row.items || {}
    }));
}

// Vía RPC (jsonb_set atómico), ver schema.sql.
export async function eliminarItemInventario(uid, codigo) {
    try {
        const { error } = await supabase.rpc("eliminar_item_inventario", {
            p_usuario: uid,
            p_codigo: codigo
        });
        if (error) throw error;
        return true;
    } catch (e) {
        console.error("❌ Error quitando item del inventario:", e);
        return false;
    }
}

export async function actualizarItemInventario(uid, codigo, item) {
    try {
        const { error } = await supabase.rpc("actualizar_item_inventario", {
            p_usuario: uid,
            p_codigo: codigo,
            p_item: item
        });
        if (error) throw error;
        return true;
    } catch (e) {
        console.error("❌ Error actualizando item del inventario:", e);
        return false;
    }
}

export async function borrarInventariosCompleto(uid) {
    const { count, error } = await supabase
        .from("inventarios_historial")
        .delete({ count: "exact" })
        .eq("usuario", uid);
    if (error) throw error;

    // En Firebase, el doc "actual" vivía en la misma colección que el
    // historial, así que borrarInventariosCompleto lo borraba de paso (y
    // después asegurarInventarioActual lo recreaba vacío). Acá "actual" está
    // en su propia tabla, así que hay que resetearlo a mano para no dejar
    // el conteo en curso con productos viejos después de un "Borrar todo".
    const { error: errorActual } = await supabase
        .from("inventario_actual")
        .update({
            estado: "cerrado",
            nombre: null,
            fecha: null,
            fecha_cierre: null,
            items: {}
        })
        .eq("usuario", uid);
    if (errorActual) throw errorActual;

    return count ?? 0;
}
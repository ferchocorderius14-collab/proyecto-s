# Prueba AES El Salvador 
-
AES EL SALVADOR - SOLUCIONES SOLARES
PROYECTO INTEGRADOR
DDL - ESQUEMA USUARIOS. 
-

CREATE SCHEMA IF NOT EXISTS usuarios;

# TABLA: usuarios.usuario

CREATE TABLE IF NOT EXISTS usuarios.usuario (
    usuario_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,

    rol VARCHAR(30) NOT NULL
        CHECK (
            rol IN (
                'administrador',
                'vendedor',
                'bodeguero',
                'comprador'
            )
        ),

    correo VARCHAR(150) NOT NULL UNIQUE,

    fecha_creacion TIMESTAMP NOT NULL DEFAULT now()
);


# TABLA: usuarios.cliente

CREATE TABLE IF NOT EXISTS usuarios.cliente (
    cliente_id SERIAL PRIMARY KEY,

    tipo_cliente VARCHAR(10) NOT NULL
        CHECK (
            tipo_cliente IN ('B2C', 'B2B')
        ),

    dui VARCHAR(10) UNIQUE,

    nombre VARCHAR(150) NOT NULL,

    correo VARCHAR(150) UNIQUE,

    telefono VARCHAR(15),

    fecha_registro TIMESTAMP NOT NULL DEFAULT now()
);


# TABLA: usuarios.empresa_perfil


CREATE TABLE IF NOT EXISTS usuarios.empresa_perfil (
    cliente_id INTEGER PRIMARY KEY,

    nrc VARCHAR(15) NOT NULL UNIQUE,

    nit VARCHAR(17) NOT NULL UNIQUE,

    giro_comercial VARCHAR(100) NOT NULL,

    CONSTRAINT fk_empresa_perfil_cliente
        FOREIGN KEY (cliente_id)
        REFERENCES usuarios.cliente(cliente_id)
        ON DELETE CASCADE
);


# INDICES

CREATE INDEX IF NOT EXISTS idx_cliente_tipo
ON usuarios.cliente(tipo_cliente);

CREATE INDEX IF NOT EXISTS idx_empresa_perfil_cliente
ON usuarios.empresa_perfil(cliente_id);
-


AES EL SALVADOR - SOLUCIONES SOLARES
PROYECTO INTEGRADOR
DDL - ESQUEMA CATALOGOS
-

CREATE SCHEMA IF NOT EXISTS catalogos;


# TABLA: catalogos.categoria

CREATE TABLE IF NOT EXISTS catalogos.categoria (
    categoria_id SERIAL PRIMARY KEY,

    categoria_padre_id INTEGER,

    nombre VARCHAR(80) NOT NULL UNIQUE,

    CONSTRAINT fk_categoria_padre
        FOREIGN KEY (categoria_padre_id)
        REFERENCES catalogos.categoria(categoria_id)
        ON DELETE SET NULL
        
);


# TABLA: catalogos.proveedor


CREATE TABLE IF NOT EXISTS catalogos.proveedor (
    proveedor_id SERIAL PRIMARY KEY,

    nombre VARCHAR(150) NOT NULL,

    nit VARCHAR(17) NOT NULL UNIQUE
);


# TABLA: catalogos.producto


CREATE TABLE IF NOT EXISTS catalogos.producto (
    producto_id SERIAL PRIMARY KEY,

    categoria_id INTEGER NOT NULL,

    proveedor_id INTEGER NOT NULL,

    sku VARCHAR(30) NOT NULL UNIQUE,

    nombre VARCHAR(150) NOT NULL,

    precio_actual NUMERIC(10,2) NOT NULL
        CHECK (precio_actual >= 0),

    estado VARCHAR(15) NOT NULL DEFAULT 'activo'
        CHECK (
            estado IN (
                'activo',
                'descontinuado'
            )
        ),

    CONSTRAINT fk_producto_categoria
        FOREIGN KEY (categoria_id)
        REFERENCES catalogos.categoria(categoria_id),

    CONSTRAINT fk_producto_proveedor
        FOREIGN KEY (proveedor_id)
        REFERENCES catalogos.proveedor(proveedor_id)
);


# INDICES

CREATE INDEX IF NOT EXISTS idx_producto_categoria
ON catalogos.producto(categoria_id);

CREATE INDEX IF NOT EXISTS idx_producto_proveedor
ON catalogos.producto(proveedor_id);

CREATE INDEX IF NOT EXISTS idx_producto_estado
ON catalogos.producto(estado);



AES EL SALVADOR - SOLUCIONES SOLARES
PROYECTO INTEGRADOR
DDL - ESQUEMA INVENTARIO
-

CREATE SCHEMA IF NOT EXISTS inventario;


# TABLA: inventario.bodega


CREATE TABLE IF NOT EXISTS inventario.bodega (
    bodega_id SERIAL PRIMARY KEY,

    nombre VARCHAR(100) NOT NULL,

    departamento VARCHAR(50) NOT NULL
);


# TABLA: inventario.inventario_bodega


CREATE TABLE IF NOT EXISTS inventario.inventario_bodega (
    producto_id INTEGER NOT NULL,

    bodega_id INTEGER NOT NULL,

    cantidad_disponible INTEGER NOT NULL
        CHECK (cantidad_disponible >= 0),

    PRIMARY KEY (producto_id, bodega_id),

    CONSTRAINT fk_inventario_producto
        FOREIGN KEY (producto_id)
        REFERENCES catalogos.producto(producto_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_inventario_bodega
        FOREIGN KEY (bodega_id)
        REFERENCES inventario.bodega(bodega_id)
        ON DELETE CASCADE
);


# TABLA: inventario.movimiento_inventario


CREATE TABLE IF NOT EXISTS inventario.movimiento_inventario (
    movimiento_id BIGSERIAL PRIMARY KEY,

    producto_id INTEGER NOT NULL,

    bodega_id INTEGER NOT NULL,

    tipo_movimiento VARCHAR(20) NOT NULL
        CHECK (
            tipo_movimiento IN (
                'compra',
                'venta',
                'traslado',
                'ajuste'
            )
        ),

    cantidad INTEGER NOT NULL
        CHECK (cantidad <> 0),

    fecha_movimiento TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_movimiento_producto
        FOREIGN KEY (producto_id)
        REFERENCES catalogos.producto(producto_id),

    CONSTRAINT fk_movimiento_bodega
        FOREIGN KEY (bodega_id)
        REFERENCES inventario.bodega(bodega_id)
);


# INDICES


CREATE INDEX IF NOT EXISTS idx_movimiento_producto_bodega_fecha
ON inventario.movimiento_inventario
(producto_id, bodega_id, fecha_movimiento);

CREATE INDEX IF NOT EXISTS idx_movimiento_tipo_fecha
ON inventario.movimiento_inventario
(tipo_movimiento, fecha_movimiento);

CREATE INDEX IF NOT EXISTS idx_inventario_bodega
ON inventario.inventario_bodega(bodega_id);

CREATE INDEX IF NOT EXISTS idx_inventario_producto
ON inventario.inventario_bodega(producto_id);




AES EL SALVADOR - SOLUCIONES SOLARES
PROYECTO INTEGRADOR
DDL - ESQUEMA COMPRAS

CREATE SCHEMA IF NOT EXISTS compras;


# TABLA: compras.orden_compra


CREATE TABLE IF NOT EXISTS compras.orden_compra (
    orden_compra_id SERIAL PRIMARY KEY,

    proveedor_id INTEGER NOT NULL,

    bodega_id INTEGER NOT NULL,

    usuario_id INTEGER NOT NULL,

    estado VARCHAR(20) NOT NULL DEFAULT 'pendiente'
        CHECK (
            estado IN (
                'pendiente',
                'recibida',
                'cancelada'
            )
        ),

    fecha_orden TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_orden_compra_proveedor
        FOREIGN KEY (proveedor_id)
        REFERENCES catalogos.proveedor(proveedor_id),

    CONSTRAINT fk_orden_compra_bodega
        FOREIGN KEY (bodega_id)
        REFERENCES inventario.bodega(bodega_id),

    CONSTRAINT fk_orden_compra_usuario
        FOREIGN KEY (usuario_id)
        REFERENCES usuarios.usuario(usuario_id)
);


# TABLA: compras.detalle_orden_compra


CREATE TABLE IF NOT EXISTS compras.detalle_orden_compra (
    detalle_orden_id SERIAL PRIMARY KEY,

    orden_compra_id INTEGER NOT NULL,

    producto_id INTEGER NOT NULL,

    cantidad INTEGER NOT NULL
        CHECK (cantidad > 0),

    costo_unitario NUMERIC(10,2) NOT NULL
        CHECK (costo_unitario >= 0),

    CONSTRAINT fk_detalle_orden_compra
        FOREIGN KEY (orden_compra_id)
        REFERENCES compras.orden_compra(orden_compra_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_detalle_orden_producto
        FOREIGN KEY (producto_id)
        REFERENCES catalogos.producto(producto_id)
);


# INDICES


CREATE INDEX IF NOT EXISTS idx_orden_proveedor
ON compras.orden_compra(proveedor_id);

CREATE INDEX IF NOT EXISTS idx_orden_bodega
ON compras.orden_compra(bodega_id);

CREATE INDEX IF NOT EXISTS idx_orden_usuario
ON compras.orden_compra(usuario_id);

CREATE INDEX IF NOT EXISTS idx_orden_fecha
ON compras.orden_compra(fecha_orden);

CREATE INDEX IF NOT EXISTS idx_detalle_orden
ON compras.detalle_orden_compra(orden_compra_id);

CREATE INDEX IF NOT EXISTS idx_detalle_orden_producto
ON compras.detalle_orden_compra(producto_id);



AES EL SALVADOR - SOLUCIONES SOLARES
PROYECTO INTEGRADOR
DDL - ESQUEMA VENTAS
-

CREATE SCHEMA IF NOT EXISTS ventas;


# TABLA: ventas.cupon


CREATE TABLE IF NOT EXISTS ventas.cupon (
    cupon_id SERIAL PRIMARY KEY,

    tipo_descuento VARCHAR(15) NOT NULL
        CHECK (
            tipo_descuento IN (
                'porcentaje',
                'monto_fijo'
            )
        ),

    valor NUMERIC(10,2) NOT NULL
        CHECK (valor > 0),

    fecha_expiracion DATE
);


TABLA: ventas.pedido
CABECERA DE LA TRANSACCION


CREATE TABLE IF NOT EXISTS ventas.pedido (
    pedido_id SERIAL PRIMARY KEY,

    cliente_id INTEGER NOT NULL,

    cupon_id INTEGER,

    estado VARCHAR(20) NOT NULL DEFAULT 'pendiente'
        CHECK (
            estado IN (
                'pendiente',
                'enviado',
                'entregado',
                'cancelado'
            )
        ),

    total NUMERIC(10,2) NOT NULL
        CHECK (total >= 0),

    fecha_pedido TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_pedido_cliente
        FOREIGN KEY (cliente_id)
        REFERENCES usuarios.cliente(cliente_id),

    CONSTRAINT fk_pedido_cupon
        FOREIGN KEY (cupon_id)
        REFERENCES ventas.cupon(cupon_id)
        ON DELETE SET NULL
);


# TABLA: ventas.detalle_pedido


CREATE TABLE IF NOT EXISTS ventas.detalle_pedido (
    detalle_pedido_id SERIAL PRIMARY KEY,

    pedido_id INTEGER NOT NULL,

    producto_id INTEGER NOT NULL,

    bodega_id INTEGER NOT NULL,

    cantidad INTEGER NOT NULL
        CHECK (cantidad > 0),

    precio_unitario NUMERIC(10,2) NOT NULL
        CHECK (precio_unitario >= 0),

    CONSTRAINT fk_detalle_pedido_pedido
        FOREIGN KEY (pedido_id)
        REFERENCES ventas.pedido(pedido_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_detalle_pedido_producto
        FOREIGN KEY (producto_id)
        REFERENCES catalogos.producto(producto_id),

    CONSTRAINT fk_detalle_pedido_bodega
        FOREIGN KEY (bodega_id)
        REFERENCES inventario.bodega(bodega_id)
);


# TABLA: ventas.pedido_historial_estado


CREATE TABLE IF NOT EXISTS ventas.pedido_historial_estado (
    historial_id BIGSERIAL PRIMARY KEY,

    pedido_id INTEGER NOT NULL,

    usuario_id INTEGER NOT NULL,

    estado_nuevo VARCHAR(20) NOT NULL,

    fecha_cambio TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_historial_pedido
        FOREIGN KEY (pedido_id)
        REFERENCES ventas.pedido(pedido_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_historial_usuario
        FOREIGN KEY (usuario_id)
        REFERENCES usuarios.usuario(usuario_id)
);


# INDICES

CREATE INDEX IF NOT EXISTS idx_pedido_cliente
ON ventas.pedido(cliente_id);

CREATE INDEX IF NOT EXISTS idx_pedido_cupon
ON ventas.pedido(cupon_id);

CREATE INDEX IF NOT EXISTS idx_pedido_estado
ON ventas.pedido(estado);

CREATE INDEX IF NOT EXISTS idx_pedido_fecha
ON ventas.pedido(fecha_pedido);

CREATE INDEX IF NOT EXISTS idx_detalle_pedido
ON ventas.detalle_pedido(pedido_id);

CREATE INDEX IF NOT EXISTS idx_detalle_producto
ON ventas.detalle_pedido(producto_id);

CREATE INDEX IF NOT EXISTS idx_detalle_bodega
ON ventas.detalle_pedido(bodega_id);

CREATE INDEX IF NOT EXISTS idx_historial_pedido
ON ventas.pedido_historial_estado(pedido_id);

CREATE INDEX IF NOT EXISTS idx_historial_usuario
ON ventas.pedido_historial_estado(usuario_id);




-- AES EL SALVADOR - SOLUCIONES SOLARES
-- PROYECTO INTEGRADOR
-- DDL - ESQUEMA FACTURACION
-

CREATE SCHEMA IF NOT EXISTS facturacion;


# TABLA: facturacion.pago


CREATE TABLE IF NOT EXISTS facturacion.pago (
    pago_id SERIAL PRIMARY KEY,

    pedido_id INTEGER NOT NULL,

    metodo_pago VARCHAR(20) NOT NULL
        CHECK (
            metodo_pago IN (
                'tarjeta',
                'transferencia',
                'efectivo'
            )
        ),

    estado_pago VARCHAR(15) NOT NULL DEFAULT 'pendiente'
        CHECK (
            estado_pago IN (
                'pendiente',
                'confirmado',
                'rechazado'
            )
        ),

    fecha_pago TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_pago_pedido
        FOREIGN KEY (pedido_id)
        REFERENCES ventas.pedido(pedido_id)
        ON DELETE CASCADE
);


# TABLA: facturacion.dte_factura


CREATE TABLE IF NOT EXISTS facturacion.dte_factura (
    dte_id SERIAL PRIMARY KEY,

    pago_id INTEGER NOT NULL UNIQUE,

    codigo_generacion VARCHAR(36) NOT NULL UNIQUE,

    total NUMERIC(10,2) NOT NULL
        CHECK (total >= 0),

    fecha_emision TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_dte_pago
        FOREIGN KEY (pago_id)
        REFERENCES facturacion.pago(pago_id)
        ON DELETE RESTRICT
);


# INDICES


CREATE INDEX IF NOT EXISTS idx_pago_pedido
ON facturacion.pago(pedido_id);

CREATE INDEX IF NOT EXISTS idx_pago_estado
ON facturacion.pago(estado_pago);

CREATE INDEX IF NOT EXISTS idx_pago_fecha
ON facturacion.pago(fecha_pago);

CREATE INDEX IF NOT EXISTS idx_dte_pago
ON facturacion.dte_factura(pago_id);

CREATE INDEX IF NOT EXISTS idx_dte_fecha
ON facturacion.dte_factura(fecha_emision);

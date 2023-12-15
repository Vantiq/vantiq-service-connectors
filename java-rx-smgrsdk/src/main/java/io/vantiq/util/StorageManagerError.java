package io.vantiq.util;

/*
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import lombok.Value;

@Value
public class StorageManagerError {
    String errorCode;
    String errorMessage;
}

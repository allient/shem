                match param.mode {
                    ParameterMode::In => {
                        // For IN parameters, only add "IN" if there's a parameter name
                        if !param.name.is_empty() {
                            param_str.push_str("IN ");
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Out => {
                        param_str.push_str("OUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::InOut => {
                        param_str.push_str("INOUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Variadic => {
                        param_str.push_str("VARIADIC ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                }

                if let Some(default) = &param.default {
                    param_str.push_str(&format!(" DEFAULT {}", default));
                } 